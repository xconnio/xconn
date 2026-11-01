//! Client session runtime. Callbacks, task cancellation and waiters stay in xconn.
use super::{
    peer::Peer,
    types::{EventFn, RegisterFn, RegisterRequest, SubscribeRequest},
};
use crate::{
    CallRequest, Error, Event, Invocation, PublishRequest, PublishResponse, RegisterResponse, RpcResult,
    SessionDetails, SubscribeResponse, WampError,
};
use futures_util::FutureExt;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, watch};
use wampproto::{
    idgen::SessionScopeIDGenerator,
    messages::{
        call::Call, cancel::Cancel, error::Error as ProtocolError, event::Event as ProtocolEvent, goodbye::Goodbye,
        interrupt::Interrupt, invocation::Invocation as ProtocolInvocation, message::Message, publish::Publish,
        register::Register, result::Result_, subscribe::Subscribe, types::Value, unregister::Unregister,
        unsubscribe::Unsubscribe, yield_::Yield as ProtocolYield,
    },
    serializers::serializer::Serializer,
};

type ContextFn = Arc<dyn Fn(InvocationContext) -> Pin<Box<dyn Future<Output = RpcResult> + Send>> + Send + Sync>;

#[derive(Clone)]
enum Handler {
    Basic(RegisterFn),
    Context(ContextFn),
}

type EventContextFn = Arc<dyn Fn(EventContext) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[derive(Clone)]
enum SubscriptionHandler {
    Basic(EventFn),
    Context(EventContextFn),
}

/// Incoming event with optional payload containers retained for foreign APIs.
pub struct EventContext {
    pub event: Event,
    pub args: Option<Vec<Value>>,
    pub kwargs: Option<HashMap<String, Value>>,
}

enum PendingAction {
    None,
    Register(Handler),
    Subscribe(i64, SubscriptionHandler),
    Unregister(i64),
    Unsubscribe(i64),
}

struct Pending {
    overflow: Arc<AtomicBool>,
    sender: mpsc::Sender<Box<dyn Message>>,
    action: PendingAction,
}

#[derive(Default)]
struct State {
    #[cfg(feature = "rpc-streams")]
    stream_opener: Option<super::peer::StreamOpener>,
    pending: HashMap<(i64, i64), Pending>,
    registrations: HashMap<i64, Handler>,
    subscriptions: HashMap<i64, Vec<(i64, SubscriptionHandler)>>,
    // None: active; Some(true): cancel with reply; Some(false): cancel silently.
    invocations: HashMap<i64, watch::Sender<Option<bool>>>,
    closing: bool,
}

struct Task(tokio::task::JoinHandle<()>, Arc<dyn Peer>, tokio::runtime::Handle);

impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
        let peer = self.1.clone();
        self.2.spawn(async move {
            let _ = peer.close().await;
        });
    }
}

#[derive(Clone)]
pub struct Session {
    details: SessionDetails,
    serializer: Arc<Box<dyn Serializer>>,
    idgen: Arc<SessionScopeIDGenerator>,
    call_order: Arc<tokio::sync::Mutex<i64>>,
    subscription_operations: Arc<tokio::sync::Mutex<()>>,
    peer: Arc<dyn Peer>,
    state: Arc<Mutex<State>>,
    disconnected: watch::Receiver<bool>,
    _task: Arc<Task>,
    executor: tokio::runtime::Handle,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session").field("details", &self.details).finish()
    }
}

#[derive(Clone)]
pub struct InvocationContext {
    pub invocation: Invocation,
    pub request_id: i64,
    cancelled: watch::Receiver<Option<bool>>,
    peer: Arc<dyn Peer>,
    serializer: Arc<Box<dyn Serializer>>,
    receive_progress: bool,
    #[cfg(feature = "rpc-streams")]
    stream_opener: Option<super::peer::StreamOpener>,
    payload_args: Option<Vec<Value>>,
    payload_kwargs: Option<HashMap<String, Value>>,
}

impl InvocationContext {
    /// Open the transport stream at the same positional index as an ordinary argument.
    #[cfg(feature = "rpc-streams")]
    pub async fn arg_stream(&self, index: usize) -> Result<super::rawsocket::BoxStream, Error> {
        let value = self
            .invocation
            .args
            .get(index)
            .ok_or_else(|| Error::new("argument index out of range"))?;
        self.open_stream_argument(value).await
    }

    #[cfg(feature = "rpc-streams")]
    pub(crate) async fn open_stream_argument(&self, value: &Value) -> Result<super::rawsocket::BoxStream, Error> {
        let open = self
            .stream_opener
            .as_ref()
            .ok_or_else(|| Error::new("transport does not support RPC streams"))?;
        super::rpc_streams::accept(open, value).await
    }

    #[cfg(all(feature = "services", feature = "rpc-streams"))]
    pub(crate) async fn offer_stream(&self) -> Result<(Value, super::rawsocket::BoxStream), Error> {
        let open = self
            .stream_opener
            .as_ref()
            .ok_or_else(|| Error::new("transport does not support RPC streams"))?;
        super::rpc_streams::offer(open).await
    }

    pub fn payload_args(&self) -> &Option<Vec<Value>> {
        &self.payload_args
    }

    pub fn payload_kwargs(&self) -> &Option<HashMap<String, Value>> {
        &self.payload_kwargs
    }

    pub async fn cancelled(&self) {
        let mut rx = self.cancelled.clone();
        let _ = rx.wait_for(|v| v.is_some()).await;
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.borrow().is_some()
    }

    pub async fn send_progress(&self, args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<(), Error> {
        self.send_progress_payload(Some(args), Some(kwargs)).await
    }

    pub(crate) async fn send_progress_payload(
        &self,
        args: Option<Vec<Value>>,
        kwargs: Option<HashMap<String, Value>>,
    ) -> Result<(), Error> {
        if !self.receive_progress {
            return Err(Error::new("caller did not request progressive results"));
        }

        if self.is_cancelled() {
            return Err(Error::new("invocation cancelled"));
        }

        let payload = self.serializer.serialize(&ProtocolYield {
            request_id: self.request_id,
            options: HashMap::from([("progress".into(), Value::Bool(true))]),
            args,
            kwargs,
        })?;
        self.peer.write(payload).await
    }
}

struct PendingGuard {
    overflow: Arc<AtomicBool>,
    state: Arc<Mutex<State>>,
    key: (i64, i64),
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.state.lock().unwrap().pending.remove(&self.key);
    }
}

/// An RPC with progressive results and optional progressive input. Dropping an
/// unfinished call requests killnowait cancellation and removes local correlation.
pub struct CallStream {
    pub request_id: i64,
    procedure: String,
    receiver: mpsc::Receiver<Box<dyn Message>>,
    session: Session,
    _guard: PendingGuard,
    finished: bool,
    input_finished: bool,
    receive_progress: bool,
    input_options: HashMap<String, Value>,
}

impl CallStream {
    pub async fn next(&mut self) -> Result<Option<RpcResult>, Error> {
        if self.finished {
            return Ok(None);
        }

        let msg = tokio::select! {
            msg = self.receiver.recv() => msg.ok_or_else(|| Error::new(
                if self._guard.overflow.load(Ordering::Acquire) { "call result queue overflow" } else { "call closed" }
            ))?,
            _ = self.session.wait_disconnect() => {
                return Err(Error::new("connection closed"));
            }
        };

        if let Some(error) = msg.as_any().downcast_ref::<ProtocolError>() {
            self.finished = true;
            return Ok(Some(RpcResult {
                error: Some(wamp_error(error)),
                ..Default::default()
            }));
        }

        let result = msg
            .as_any()
            .downcast_ref::<Result_>()
            .ok_or_else(|| Error::new("unexpected call reply"))?;
        let progressive = result.details.get("progress") == Some(&Value::Bool(true));
        if progressive && !self.receive_progress {
            let _ = self.session.close().await;
            return Err(Error::new("received an unsolicited progressive result"));
        }
        self.finished = !progressive;
        Ok(Some(RpcResult {
            args: result.args.clone(),
            kwargs: result.kwargs.clone(),
            details: result.details.clone(),
            error: None,
        }))
    }

    pub async fn cancel(&self, mode: &str) -> Result<(), Error> {
        if !matches!(mode, "skip" | "kill" | "killnowait") {
            return Err(Error::new("invalid cancellation mode"));
        }

        self.session
            .send(&Cancel {
                request_id: self.request_id,
                options: HashMap::from([("mode".into(), Value::Str(mode.into()))]),
            })
            .await
    }

    pub async fn send_input(
        &mut self,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
        more: bool,
    ) -> Result<(), Error> {
        if self.finished || self.input_finished {
            return Err(Error::new("call input finished"));
        }

        self.input_options.insert("progress".into(), Value::Bool(more));
        self.input_finished = !more;
        self.session
            .send(&Call {
                request_id: self.request_id,
                procedure: self.procedure.clone(),
                options: self.input_options.clone(),
                args: Some(args),
                kwargs: Some(kwargs),
            })
            .await
    }
}

impl Drop for CallStream {
    fn drop(&mut self) {
        if !self.finished && self.session.connected() {
            let session = self.session.clone();
            let request_id = self.request_id;
            self.session.executor.spawn(async move {
                let _ = session
                    .send(&Cancel {
                        request_id,
                        options: HashMap::from([("mode".into(), Value::Str("killnowait".into()))]),
                    })
                    .await;
            });
        }
    }
}

fn wamp_error(error: &ProtocolError) -> WampError {
    WampError {
        uri: error.uri.clone(),
        args: error.args.clone(),
        kwargs: error.kwargs.clone(),
    }
}

// Begin callbacks in wire order. Pending work continues independently on Tokio,
// so nested client calls can still make progress through the receive loop.
fn spawn_callback(future: impl Future<Output = ()> + Send + 'static) {
    let mut future = Box::pin(std::panic::AssertUnwindSafe(future).catch_unwind());

    if future.as_mut().now_or_never().is_none() {
        tokio::spawn(future);
    }
}

impl Session {
    /// Build a call without sending it. Execute the resulting request explicitly.
    pub fn call(&self, procedure: &str) -> super::requests::CallBuilder<'_> {
        super::requests::CallBuilder {
            session: self,
            request: crate::CallRequest::new(procedure),
        }
    }

    /// Build a publication without sending it.
    pub fn publish(&self, topic: &str) -> super::requests::PublishBuilder<'_> {
        super::requests::PublishBuilder {
            session: self,
            request: crate::PublishRequest::new(topic),
        }
    }

    pub fn register<F, Fut>(&self, procedure: &str, handler: F) -> super::requests::RegisterBuilder<'_>
    where
        F: Fn(crate::Invocation) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = crate::RpcResult> + Send + 'static,
    {
        super::requests::RegisterBuilder {
            session: self,
            request: RegisterRequest::new(procedure, handler),
        }
    }

    pub fn subscribe<F, Fut>(&self, topic: &str, handler: F) -> super::requests::SubscribeBuilder<'_>
    where
        F: Fn(crate::Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        super::requests::SubscribeBuilder {
            session: self,
            request: SubscribeRequest::new(topic, handler),
        }
    }

    pub fn new(details: SessionDetails, peer: Box<dyn Peer>, serializer: Box<dyn Serializer>) -> Self {
        let peer: Arc<dyn Peer> = Arc::from(peer);
        let serializer = Arc::new(serializer);
        let state = Arc::new(Mutex::new(State {
            #[cfg(feature = "rpc-streams")]
            stream_opener: peer.stream_opener(),
            ..Default::default()
        }));
        let (done, disconnected) = watch::channel(false);
        let (task_peer, task_serializer, task_state) = (peer.clone(), serializer.clone(), state.clone());
        let task = tokio::spawn(async move {
            loop {
                let msg = match task_peer
                    .read()
                    .await
                    .and_then(|data| task_serializer.deserialize(data).map_err(Error::from))
                {
                    Ok(msg) => msg,
                    Err(_) => break,
                };

                if !Self::receive(msg, &task_state, &task_serializer, &task_peer).await {
                    break;
                }
            }

            {
                let mut state = task_state.lock().unwrap();
                state.pending.clear();
                state.registrations.clear();
                state.subscriptions.clear();

                for (_, cancel) in state.invocations.drain() {
                    cancel.send_replace(Some(false));
                }
            }

            done.send_replace(true);
            let _ = task_peer.close().await;
        });
        Self {
            details,
            peer: peer.clone(),
            serializer,
            state,
            idgen: Arc::new(SessionScopeIDGenerator::new()),
            call_order: Arc::new(tokio::sync::Mutex::new(0)),
            subscription_operations: Arc::new(tokio::sync::Mutex::new(())),
            disconnected,
            _task: Arc::new(Task(task, peer.clone(), tokio::runtime::Handle::current())),
            executor: tokio::runtime::Handle::current(),
        }
    }

    async fn receive(
        msg: Box<dyn Message>,
        state: &Arc<Mutex<State>>,
        serializer: &Arc<Box<dyn Serializer>>,
        peer: &Arc<dyn Peer>,
    ) -> bool {
        let code = match msg.message_type() {
            Value::Int(code) => code,
            _ => return false,
        };
        let mut callback = None;
        let mut invocation = None;
        let mut events = Vec::new();
        let mut goodbye_reply = false;
        {
            let mut state = state.lock().unwrap();

            match code {
                6 => {
                    goodbye_reply = !state.closing;
                    state.closing = true;
                }
                3 => return false,
                68 => {
                    let v = msg.as_any().downcast_ref::<ProtocolInvocation>().unwrap();

                    if let Some(handler) = state.registrations.get(&v.registration_id).cloned() {
                        let cancelled = state
                            .invocations
                            .entry(v.request_id)
                            .or_insert_with(|| watch::channel(None).0)
                            .subscribe();
                        callback = Some(handler);
                        invocation = Some(InvocationContext {
                            invocation: Invocation {
                                args: v.args.clone().unwrap_or_default(),
                                kwargs: v.kwargs.clone().unwrap_or_default(),
                                details: v.details.clone(),
                            },
                            request_id: v.request_id,
                            #[cfg(feature = "rpc-streams")]
                            stream_opener: state.stream_opener.clone(),
                            payload_args: v.args.clone(),
                            payload_kwargs: v.kwargs.clone(),
                            cancelled,
                            peer: peer.clone(),
                            serializer: serializer.clone(),
                            receive_progress: v.details.get("receive_progress") == Some(&Value::Bool(true)),
                        });
                    }
                }
                69 => {
                    let v = msg.as_any().downcast_ref::<Interrupt>().unwrap();

                    if let Some(cancel) = state.invocations.remove(&v.request_id) {
                        cancel.send_replace(Some(v.options.get("mode") == Some(&Value::Str("kill".into()))));
                    }
                }
                36 => {
                    let v = msg.as_any().downcast_ref::<ProtocolEvent>().unwrap();

                    if let Some(handlers) = state.subscriptions.get(&v.subscription_id) {
                        for (_, handler) in handlers {
                            events.push((
                                handler.clone(),
                                EventContext {
                                    args: v.args.clone(),
                                    kwargs: v.kwargs.clone(),
                                    event: Event {
                                        args: v.args.clone().unwrap_or_default(),
                                        kwargs: v.kwargs.clone().unwrap_or_default(),
                                        details: v.details.clone(),
                                    },
                                },
                            ));
                        }
                    }
                }
                _ => {
                    let wire = msg.marshal();
                    let (kind, id) = if code == 8 {
                        let error = msg.as_any().downcast_ref::<ProtocolError>().unwrap();
                        (
                            match error.message_type {
                                Value::Int(v) => v,
                                _ => return false,
                            },
                            error.request_id,
                        )
                    } else {
                        let kind = match code {
                            17 => 16,
                            33 => 32,
                            35 => 34,
                            50 => 48,
                            65 => 64,
                            67 => 66,
                            _ => return false,
                        };
                        (
                            kind,
                            match wire.get(1) {
                                Some(Value::Int(v)) => *v,
                                _ => return false,
                            },
                        )
                    };
                    let key = (kind, id);
                    let progress = code == 50
                        && msg.as_any().downcast_ref::<Result_>().unwrap().details.get("progress")
                            == Some(&Value::Bool(true));
                    if progress {
                        if let Some(pending) = state.pending.get(&key)
                            && pending.sender.try_send(msg).is_err()
                        {
                            pending.overflow.store(true, Ordering::Release);
                            state.pending.remove(&key);
                        }

                        return true;
                    }

                    if let Some(pending) = state.pending.remove(&key) {
                        if code != 8 {
                            match pending.action {
                                PendingAction::Register(handler) => {
                                    if let Some(Value::Int(id)) = wire.get(2) {
                                        state.registrations.insert(*id, handler);
                                    }
                                }
                                PendingAction::Subscribe(token, handler) => {
                                    if let Some(Value::Int(id)) = wire.get(2) {
                                        state.subscriptions.entry(*id).or_default().push((token, handler));
                                    }
                                }
                                PendingAction::Unregister(id) => {
                                    state.registrations.remove(&id);
                                }
                                PendingAction::Unsubscribe(id) => {
                                    state.subscriptions.remove(&id);
                                }
                                PendingAction::None => {}
                            }
                        }

                        if pending.sender.try_send(msg).is_err() {
                            pending.overflow.store(true, Ordering::Release);
                        }
                    }
                }
            }
        }

        for (handler, event) in events {
            spawn_callback(async move {
                match handler {
                    SubscriptionHandler::Basic(handler) => handler.invoke(event.event).await,
                    SubscriptionHandler::Context(handler) => handler(event).await,
                }
            });
        }

        if let (Some(callback), Some(context)) = (callback, invocation) {
            let state = state.clone();
            spawn_callback(async move {
                let invoke = async {
                    match callback {
                        Handler::Basic(handler) => handler.invoke(context.invocation.clone()).await,
                        Handler::Context(handler) => handler(context.clone()).await,
                    }
                };
                let response = tokio::select! {
                    biased;
                    response = invoke => response,
                    _ = context.cancelled() => RpcResult::error("wamp.error.canceled"),
                };

                if context.is_cancelled() {
                    // kill requires acknowledgement; killnowait and connection
                    // teardown must not send a response to the interrupted call.
                    if *context.cancelled.borrow() != Some(true) {
                        return;
                    }

                    let data = context.serializer.serialize(&ProtocolError {
                        message_type: Value::Int(68),
                        request_id: context.request_id,
                        options: HashMap::new(),
                        uri: "wamp.error.canceled".into(),
                        args: None,
                        kwargs: None,
                    });

                    if let Ok(data) = data {
                        let _ = context.peer.write(data).await;
                    }

                    return;
                }

                if response.error.as_ref().is_some_and(|e| e.uri == "io.xconn.no_result") {
                    return;
                }

                let msg: Box<dyn Message> = if let Some(error) = response.error {
                    Box::new(ProtocolError {
                        message_type: Value::Int(68),
                        request_id: context.request_id,
                        options: HashMap::new(),
                        uri: error.uri,
                        args: error.args,
                        kwargs: error.kwargs,
                    })
                } else {
                    Box::new(ProtocolYield {
                        request_id: context.request_id,
                        options: HashMap::new(),
                        args: response.args,
                        kwargs: response.kwargs,
                    })
                };
                let data = context.serializer.serialize(msg.as_ref());

                if let Ok(data) = data {
                    let _ = context.peer.write(data).await;
                }

                state.lock().unwrap().invocations.remove(&context.request_id);
            });
        }

        if code == 6 {
            if goodbye_reply {
                let data = serializer.serialize(&Goodbye {
                    details: HashMap::new(),
                    reason: "wamp.close.goodbye_and_out".into(),
                });

                if let Ok(data) = data {
                    let _ = peer.write(data).await;
                }
            }

            return false;
        }

        true
    }

    #[cfg(all(
        feature = "rpc-streams",
        any(feature = "quic", feature = "webtransport", feature = "yamux")
    ))]
    pub(crate) fn with_stream_opener(self, open: super::peer::StreamOpener) -> Self {
        self.state.lock().unwrap().stream_opener = Some(open);
        self
    }

    /// Create an owned stream and its positional RPC argument reference.
    /// Byte traffic uses the transport; WAMP carries only the reference.
    #[cfg(feature = "rpc-streams")]
    pub async fn offer_stream(&self) -> Result<(Value, super::rawsocket::BoxStream), Error> {
        let open = self
            .state
            .lock()
            .unwrap()
            .stream_opener
            .clone()
            .ok_or_else(|| Error::new("transport does not support RPC streams"))?;
        super::rpc_streams::offer(&open).await
    }

    /// Open a stream reference received in invocation or result arguments.
    #[cfg(feature = "rpc-streams")]
    pub async fn open_stream_argument(&self, value: &Value) -> Result<super::rawsocket::BoxStream, Error> {
        let open = self
            .state
            .lock()
            .unwrap()
            .stream_opener
            .clone()
            .ok_or_else(|| Error::new("transport does not support RPC streams"))?;
        super::rpc_streams::accept(&open, value).await
    }

    pub fn connected(&self) -> bool {
        !*self.disconnected.borrow() && !self.state.lock().unwrap().closing
    }

    pub fn details(&self) -> &SessionDetails {
        &self.details
    }

    pub fn send(&self, msg: &dyn Message) -> impl Future<Output = Result<(), Error>> + Send + '_ {
        let payload = self.serializer.serialize(msg).map_err(Error::from);
        async move { self.peer.write(payload?).await }
    }

    fn pending(&self, kind: i64, id: i64, action: PendingAction) -> (PendingGuard, mpsc::Receiver<Box<dyn Message>>) {
        let (sender, receiver) = mpsc::channel(32);
        let key = (kind, id);
        let overflow = Arc::new(AtomicBool::new(false));
        self.state.lock().unwrap().pending.insert(
            key,
            Pending {
                sender,
                action,
                overflow: overflow.clone(),
            },
        );
        (
            PendingGuard {
                overflow,
                state: self.state.clone(),
                key,
            },
            receiver,
        )
    }

    async fn request(
        &self,
        msg: Box<dyn Message>,
        kind: i64,
        id: i64,
        action: PendingAction,
    ) -> Result<Box<dyn Message>, Error> {
        if !self.connected() {
            return Err(Error::new("session not established"));
        }

        let (_guard, mut receiver) = self.pending(kind, id, action);
        self.send(msg.as_ref()).await?;
        tokio::select! {
            result = receiver.recv() => result.ok_or_else(|| Error::new("request closed")),
            _ = self.wait_disconnect() => Err(Error::new("connection closed")),
            _ = tokio::time::sleep(Duration::from_secs(10)) => Err(Error::new("request timed out")),
        }
    }

    pub async fn call_stream(&self, request: CallRequest) -> Result<CallStream, Error> {
        if !self.connected() {
            return Err(Error::new("session not established"));
        }

        // Allocate and send new CALL IDs in the same order. Continuations retain
        // their original ID and can interleave with newer calls.
        let mut last_call = self.call_order.lock().await;
        let id = self.idgen.next_id();
        if id <= *last_call {
            return Err(Error::new("session call ID space exhausted; reconnect"));
        }
        *last_call = id;

        let mut msg = request.to_call(id);
        msg.options
            .entry("receive_progress".into())
            .or_insert(Value::Bool(true));
        let (guard, receiver) = self.pending(48, id, PendingAction::None);
        let procedure = msg.procedure.clone();
        let stream = CallStream {
            request_id: id,
            procedure,
            receiver,
            session: self.clone(),
            _guard: guard,
            finished: false,
            input_finished: msg.options.get("progress") != Some(&Value::Bool(true)),
            receive_progress: msg.options.get("receive_progress") == Some(&Value::Bool(true)),
            input_options: msg.options.clone(),
        };
        self.send(&msg).await?;
        Ok(stream)
    }

    #[doc(hidden)]
    pub async fn execute_call(&self, request: CallRequest) -> Result<RpcResult, Error> {
        if request.request_options().get("progress") == Some(&Value::Bool(true)) {
            return Err(Error::new("progressive input requires stream()"));
        }

        let mut stream = self.call_stream(request).await?;

        loop {
            let result = stream.next().await?.ok_or_else(|| Error::new("call closed"))?;

            if stream.finished {
                return Ok(result);
            }
        }
    }

    #[doc(hidden)]
    pub async fn execute_publish(&self, request: PublishRequest) -> Result<Option<PublishResponse>, Error> {
        let id = self.idgen.next_id();
        let msg: Publish = request.to_publish(id);

        if msg.options.get("acknowledge") != Some(&Value::Bool(true)) {
            if !self.connected() {
                return Err(Error::new("session not established"));
            }

            self.send(&msg).await?;
            return Ok(None);
        }

        let result = self.request(Box::new(msg), 16, id, PendingAction::None).await?;
        Ok(Some(PublishResponse {
            error: result.as_any().downcast_ref::<ProtocolError>().map(wamp_error),
        }))
    }

    #[doc(hidden)]
    pub async fn execute_register(&self, request: RegisterRequest) -> Result<RegisterResponse, Error> {
        self.register_handler(
            request.procedure(),
            request.options().clone(),
            Handler::Basic(request.callback()),
        )
        .await
    }

    pub async fn register_context<F, Fut>(
        &self,
        procedure: impl Into<String>,
        options: HashMap<String, Value>,
        handler: F,
    ) -> Result<RegisterResponse, Error>
    where
        F: Fn(InvocationContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = RpcResult> + Send + 'static,
    {
        self.register_handler(
            procedure.into(),
            options,
            Handler::Context(Arc::new(move |context| Box::pin(handler(context)))),
        )
        .await
    }

    async fn register_handler(
        &self,
        procedure: String,
        options: HashMap<String, Value>,
        handler: Handler,
    ) -> Result<RegisterResponse, Error> {
        let id = self.idgen.next_id();
        let reply = self
            .request(
                Box::new(Register {
                    request_id: id,
                    options,
                    procedure,
                }),
                64,
                id,
                PendingAction::Register(handler),
            )
            .await?;
        if let Some(error) = reply.as_any().downcast_ref::<ProtocolError>() {
            return Ok(RegisterResponse {
                registration_id: 0,
                error: Some(wamp_error(error)),
            });
        }

        Ok(RegisterResponse {
            registration_id: reply
                .as_any()
                .downcast_ref::<wampproto::messages::registered::Registered>()
                .unwrap()
                .registration_id,
            error: None,
        })
    }

    pub async fn unregister(&self, id: i64) -> Result<Option<WampError>, Error> {
        let request_id = self.idgen.next_id();
        let reply = self
            .request(
                Box::new(Unregister {
                    request_id,
                    registration_id: id,
                }),
                66,
                request_id,
                PendingAction::Unregister(id),
            )
            .await?;
        Ok(reply.as_any().downcast_ref::<ProtocolError>().map(wamp_error))
    }

    #[doc(hidden)]
    pub async fn execute_subscribe(&self, request: SubscribeRequest) -> Result<SubscribeResponse, Error> {
        self.subscribe_inner(request, self.idgen.next_id()).await
    }

    pub async fn subscribe_handle(&self, request: SubscribeRequest) -> Result<Subscription, Error> {
        let token = self.idgen.next_id();
        let response = self.subscribe_inner(request, token).await?;

        if let Some(error) = response.error {
            return Err(Error::new(error.uri));
        }

        Ok(Subscription {
            session: self.clone(),
            id: response.subscription_id,
            token,
        })
    }

    async fn subscribe_inner(&self, request: SubscribeRequest, id: i64) -> Result<SubscribeResponse, Error> {
        self.subscribe_handler(
            request.topic(),
            request.options().clone(),
            SubscriptionHandler::Basic(request.callback()),
            id,
        )
        .await
    }

    pub async fn subscribe_context<F, Fut>(
        &self,
        topic: String,
        options: HashMap<String, Value>,
        handler: F,
    ) -> Result<Subscription, Error>
    where
        F: Fn(EventContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let token = self.idgen.next_id();
        let handler = SubscriptionHandler::Context(Arc::new(move |event| Box::pin(handler(event))));
        let response = self.subscribe_handler(topic, options, handler, token).await?;

        if let Some(error) = response.error {
            return Err(Error::new(error.uri));
        }

        Ok(Subscription {
            session: self.clone(),
            id: response.subscription_id,
            token,
        })
    }

    async fn subscribe_handler(
        &self,
        topic: String,
        options: HashMap<String, Value>,
        handler: SubscriptionHandler,
        id: i64,
    ) -> Result<SubscribeResponse, Error> {
        let session = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.executor.spawn(async move {
            let _operation = session.subscription_operations.lock().await;
            if sender.is_closed() {
                return;
            }

            let response = session.subscribe_handler_inner(topic, options, handler, id).await;
            if response.is_err() {
                // A failed/timed-out exchange leaves remote subscription state
                // unknown. Closing the transport makes that state unambiguous.
                let _ = session.close().await;
            }
            let output = response.map(|response| SubscriptionOutput {
                session: session.clone(),
                response: Some(response),
                token: id,
            });
            let _ = sender.send(output);
        });
        let mut output = receiver
            .await
            .map_err(|_| Error::new("subscription operation stopped"))??;
        Ok(output.response.take().unwrap())
    }

    async fn subscribe_handler_inner(
        &self,
        topic: String,
        options: HashMap<String, Value>,
        handler: SubscriptionHandler,
        id: i64,
    ) -> Result<SubscribeResponse, Error> {
        let reply = self
            .request(
                Box::new(Subscribe {
                    request_id: id,
                    options,
                    topic,
                }),
                32,
                id,
                PendingAction::Subscribe(id, handler),
            )
            .await?;
        if let Some(error) = reply.as_any().downcast_ref::<ProtocolError>() {
            return Ok(SubscribeResponse {
                subscription_id: 0,
                error: Some(wamp_error(error)),
            });
        }

        Ok(SubscribeResponse {
            subscription_id: reply
                .as_any()
                .downcast_ref::<wampproto::messages::subscribed::Subscribed>()
                .unwrap()
                .subscription_id,
            error: None,
        })
    }

    pub async fn unsubscribe(&self, id: i64) -> Result<Option<WampError>, Error> {
        self.unsubscribe_token(id, None).await
    }

    async fn unsubscribe_token(&self, id: i64, token: Option<i64>) -> Result<Option<WampError>, Error> {
        let session = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.executor.spawn(async move {
            let _operation = session.subscription_operations.lock().await;
            if sender.is_closed() {
                return;
            }

            if let Some(token) = token {
                let mut state = session.state.lock().unwrap();
                let Some(handlers) = state.subscriptions.get_mut(&id) else {
                    let _ = sender.send(Ok(None));
                    return;
                };
                if !handlers.iter().any(|(existing, _)| *existing == token) {
                    let _ = sender.send(Ok(None));
                    return;
                }
                if handlers.len() > 1 {
                    handlers.retain(|(existing, _)| *existing != token);
                    let _ = sender.send(Ok(None));
                    return;
                }
            }

            let response = session.unsubscribe_inner(id).await;
            if response.is_err() {
                let _ = session.close().await;
            }
            let _ = sender.send(response);
        });
        receiver
            .await
            .map_err(|_| Error::new("unsubscribe operation stopped"))?
    }

    async fn unsubscribe_inner(&self, id: i64) -> Result<Option<WampError>, Error> {
        let request_id = self.idgen.next_id();
        let reply = self
            .request(
                Box::new(Unsubscribe {
                    request_id,
                    subscription_id: id,
                }),
                34,
                request_id,
                PendingAction::Unsubscribe(id),
            )
            .await?;
        Ok(reply.as_any().downcast_ref::<ProtocolError>().map(wamp_error))
    }

    pub async fn leave(&self) -> Result<(), Error> {
        {
            let mut state = self.state.lock().unwrap();

            if state.closing {
                return Err(Error::new("session already closing"));
            }

            state.closing = true;
        }

        self.send(&Goodbye {
            details: HashMap::new(),
            reason: "wamp.close.close_realm".into(),
        })
        .await?;
        tokio::time::timeout(Duration::from_secs(10), self.wait_disconnect())
            .await
            .map_err(|_| Error::new("leave timeout"))?;
        Ok(())
    }

    pub async fn ping_with_timeout(&self, timeout: Option<Duration>) -> Result<Duration, Error> {
        self.peer.ping(timeout).await
    }

    pub async fn ping(&self, timeout: Duration) -> Result<Duration, Error> {
        self.peer.ping(Some(timeout)).await
    }

    pub async fn close(&self) -> Result<(), Error> {
        self.peer.close().await
    }

    pub async fn wait_disconnect(&self) {
        let mut rx = self.disconnected.clone();
        let _ = rx.wait_for(|v| *v).await;
    }
}

/// An individual local subscription. Several handles can share one router ID.
pub struct Subscription {
    session: Session,
    pub id: i64,
    token: i64,
}

impl Subscription {
    pub async fn unsubscribe(&self) -> Result<Option<WampError>, Error> {
        self.session.unsubscribe_token(self.id, Some(self.token)).await
    }
}

// Own a successful subscription until its result reaches the caller. Dropping
// the caller's future cannot leave an acknowledged handler/resource orphaned.
struct SubscriptionOutput {
    session: Session,
    response: Option<SubscribeResponse>,
    token: i64,
}

impl Drop for SubscriptionOutput {
    fn drop(&mut self) {
        if let Some(response) = self.response.take()
            && response.error.is_none()
        {
            let session = self.session.clone();
            let token = self.token;
            self.session.executor.spawn(async move {
                let _ = session.unsubscribe_token(response.subscription_id, Some(token)).await;
            });
        }
    }
}
