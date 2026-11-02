//! RPC forwarding without application-specific identity or authorization policy.
use crate::async_::{
    call::Call,
    session::{InvocationContext, Session, Subscription},
    types::SubscribeRequest,
};
use crate::wampproto::messages::schema::WireValue;
use crate::{CallRequest, Error, RpcResult, StreamOptions, Value, WampError};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex as SyncMutex},
};
use tokio::sync::{Mutex, watch};

/// The application chooses how a disclosed caller obtains a backend session.
/// Sessions returned by this factory are owned by the bridge.
pub type SessionFactory =
    Arc<dyn Fn(Caller) -> Pin<Box<dyn Future<Output = Result<Session, Error>> + Send>> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct Caller {
    pub session_id: i64,
    pub authid: String,
    pub authrole: String,
}

#[derive(Clone, Debug)]
pub struct BridgeRoute {
    pub procedure: String,
    pub options: HashMap<String, Value>,
}

impl BridgeRoute {
    pub fn exact(procedure: impl Into<String>) -> Self {
        Self {
            procedure: procedure.into(),
            options: HashMap::new(),
        }
    }

    pub fn prefix(prefix: impl Into<String>) -> Self {
        Self {
            procedure: prefix.into(),
            options: HashMap::from([("match".into(), Value::Str("prefix".into()))]),
        }
    }
}

struct Callers {
    closed: bool,
    sessions: HashMap<i64, Arc<Mutex<Option<Session>>>>,
}

struct ForwardedCall {
    rpc: Call,
    target: Session,
}

enum CallState {
    Finished,
    Active(Arc<ForwardedCall>),
}

type CallSlot = Arc<Mutex<Option<CallState>>>;
type ForwardedCalls = Arc<Mutex<HashMap<i64, CallSlot>>>;

#[derive(Clone, Copy)]
enum StreamDirection {
    Input,
    Result,
}

// Roll back partially translated frames; committed relays stay bridge-owned.
#[derive(Default)]
struct StreamRelays(Vec<tokio::task::AbortHandle>);

impl StreamRelays {
    fn commit(mut self) {
        self.0.clear();
    }
}

impl Drop for StreamRelays {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

struct ForwardingGuard {
    context: InvocationContext,
    id: i64,
    call: Option<Arc<ForwardedCall>>,
    slot: CallSlot,
    calls: ForwardedCalls,
}

impl Drop for ForwardingGuard {
    fn drop(&mut self) {
        if let Some(call) = &self.call
            && !call.rpc.is_finished()
        {
            call.rpc.cancel();
        }
        let (id, slot, calls) = (self.id, self.slot.clone(), self.calls.clone());
        let context = self.context.clone();
        tokio::spawn(async move {
            // A backend may finish before the caller's final input. Retain its
            // completed call until the source invocation sends its final reply
            // or is interrupted, so already-queued chunks cannot reopen it.
            context.cancelled().await;
            let mut calls = calls.lock().await;
            if calls.get(&id).is_some_and(|current| Arc::ptr_eq(current, &slot)) {
                calls.remove(&id);
            }
        });
    }
}

struct Forwarder {
    calls: ForwardedCalls,
    #[cfg(feature = "rpc-streams")]
    streams: Mutex<tokio::task::JoinSet<()>>,
    target: Session,
    factory: Option<SessionFactory>,
    callers: SyncMutex<Callers>,
    stopped: watch::Sender<bool>,
}

impl Forwarder {
    async fn target(&self, context: &InvocationContext) -> Result<Session, Error> {
        let Some(factory) = &self.factory else {
            return Ok(self.target.clone());
        };
        let fields = &context.invocation.details;
        let session_id = fields
            .get("caller")
            .and_then(Value::integer)
            .filter(|id| *id > 0)
            .ok_or_else(|| Error::new("caller disclosure is required"))?;
        let slot = {
            let mut callers = self.callers.lock().unwrap();
            if callers.closed {
                return Err(Error::new("bridge closed"));
            }
            callers.sessions.entry(session_id).or_default().clone()
        };
        let mut cached = slot.lock().await;
        if let Some(session) = &*cached {
            return Ok(session.clone());
        }
        let text = |key: &str| match fields.get(key) {
            Some(Value::Str(value)) => value.clone(),
            _ => String::new(),
        };
        let session = factory(Caller {
            session_id,
            authid: text("caller_authid"),
            authrole: text("caller_authrole"),
        })
        .await?;
        let attached = {
            let callers = self.callers.lock().unwrap();
            !callers.closed
                && callers
                    .sessions
                    .get(&session_id)
                    .is_some_and(|current| Arc::ptr_eq(current, &slot))
        };
        if !attached {
            let _ = session.close().await;
            return Err(Error::new("caller left during backend session creation"));
        }
        *cached = Some(session.clone());
        Ok(session)
    }

    async fn forward_streams(
        &self,
        context: &InvocationContext,
        target: &Session,
        args: &mut Option<Vec<Value>>,
        direction: StreamDirection,
    ) -> Result<StreamRelays, Error> {
        let relays = StreamRelays::default();
        #[cfg(feature = "rpc-streams")]
        let mut relays = relays;
        #[cfg(not(feature = "rpc-streams"))]
        let _ = (context, target, args, direction);
        #[cfg(feature = "rpc-streams")]
        for value in args.iter_mut().flatten() {
            if crate::async_::rpc_streams::token(value)?.is_none() {
                continue;
            }
            let (reference, incoming, outgoing) = match direction {
                StreamDirection::Input => {
                    let incoming = context.open_stream_argument(value).await?;
                    let (reference, outgoing) = target.offer_stream().await?;
                    (reference, incoming, outgoing)
                }
                StreamDirection::Result => {
                    let incoming = target.open_stream_argument(value).await?;
                    let (reference, outgoing) = context.offer_stream().await?;
                    (reference, incoming, outgoing)
                }
            };
            let mut streams = self.streams.lock().await;
            if *self.stopped.borrow() {
                return Err(Error::new("bridge closed"));
            }
            // Reap finished transfers without retaining their task records for
            // the lifetime of a long-running bridge.
            while streams.try_join_next().is_some() {}
            let mut stopped = self.stopped.subscribe();
            let context = context.clone();
            let target = target.clone();
            let task = streams.spawn(async move {
                let cancelled = async {
                    context.cancelled().await;
                    if matches!(direction, StreamDirection::Result) && !context.is_cancelled() {
                        // Normal RPC completion releases the invocation watch.
                        // A returned stream must survive that final reply.
                        std::future::pending::<()>().await;
                    }
                };
                tokio::select! {
                    biased;
                    _ = stopped.wait_for(|stopped| *stopped) => {},
                    _ = target.wait_disconnect() => {},
                    _ = cancelled => {},
                    result = crate::async_::rpc_streams::forward(incoming, outgoing) => {
                        if let Err(error) = result {
                            tracing::warn!(%error, "RPC stream forwarding failed");
                        }
                    },
                }
            });
            relays.0.push(task);
            *value = reference;
        }
        Ok(relays)
    }

    async fn forward(&self, context: InvocationContext, procedure: String, prefix: bool) -> RpcResult {
        let mut stopped = self.stopped.subscribe();
        tokio::select! {
            biased;
            _ = stopped.wait_for(|stopped| *stopped) => RpcResult::error("wamp.error.canceled"),
            _ = context.cancelled() => RpcResult::error("wamp.error.canceled"),
            result = self.forward_call(&context, procedure, prefix) => result,
        }
    }

    async fn forward_call(&self, context: &InvocationContext, procedure: String, prefix: bool) -> RpcResult {
        let more = context.invocation.details.get("progress") == Some(&Value::Bool(true));
        let slot = self.calls.lock().await.entry(context.request_id).or_default().clone();
        let mut state = slot.lock().await;
        if let Some(existing) = &*state {
            let call = match existing {
                CallState::Finished => return RpcResult::no_result(),
                CallState::Active(call) => call.clone(),
            };
            drop(state);
            if call.rpc.is_finished() {
                return RpcResult::no_result();
            }
            let mut args = context.payload_args().clone();
            let relays = match self
                .forward_streams(context, &call.target, &mut args, StreamDirection::Input)
                .await
            {
                Ok(relays) => relays,
                Err(_) => {
                    call.rpc.cancel();
                    return RpcResult::no_result();
                }
            };
            let args = args.unwrap_or_default();
            let kwargs = context.payload_kwargs().clone().unwrap_or_default();
            let result = if more {
                call.rpc.send(args, kwargs).await
            } else {
                call.rpc.finish(args, kwargs).await
            };
            if result.is_ok() {
                relays.commit();
            } else if !call.rpc.is_finished() {
                call.rpc.cancel();
            }
            // The first handler owns the result path and sends the terminal reply.
            return RpcResult::no_result();
        }
        // Reserve this invocation before awaiting. Later chunks wait on this
        // slot only; unrelated invocations never share its initialization lock.
        *state = Some(CallState::Finished);
        let mut guard = ForwardingGuard {
            context: context.clone(),
            id: context.request_id,
            call: None,
            slot: slot.clone(),
            calls: self.calls.clone(),
        };
        let target = match self.target(context).await {
            Ok(target) => target,
            Err(error) => return failure(error),
        };
        let procedure = if prefix {
            match context.invocation.details.get("procedure") {
                Some(Value::Str(procedure)) => procedure.clone(),
                _ => return RpcResult::error("wamp.error.invalid_argument"),
            }
        } else {
            procedure
        };
        let mut request = CallRequest::new(procedure);
        let mut args = context.payload_args().clone();
        let _input_relays = match self
            .forward_streams(context, &target, &mut args, StreamDirection::Input)
            .await
        {
            Ok(relays) => relays,
            Err(error) => return failure(error),
        };
        if let Some(args) = args {
            request = request.args(args);
        }
        if let Some(kwargs) = context.payload_kwargs() {
            request = request.kwargs(kwargs.clone());
        }
        let options = StreamOptions {
            progressive_input: more,
            progressive_results: context.invocation.details.get("receive_progress") == Some(&Value::Bool(true)),
            ..Default::default()
        };
        let call = match Call::open(&target, request, options).await {
            Ok(call) => call,
            Err(error) => return failure(error),
        };
        let call = Arc::new(ForwardedCall { rpc: call, target });
        guard.call = Some(call.clone());
        *state = Some(CallState::Active(call.clone()));
        drop(state);
        self.receive_results(context, &call.rpc, &call.target).await
    }

    async fn receive_results(&self, context: &InvocationContext, call: &Call, target: &Session) -> RpcResult {
        let mut stopped = self.stopped.subscribe();
        let results = async {
            loop {
                match call.receive().await {
                    Ok(Some(mut result)) => {
                        let relays = match self
                            .forward_streams(context, target, &mut result.args, StreamDirection::Result)
                            .await
                        {
                            Ok(relays) => relays,
                            Err(error) => return failure(error),
                        };
                        if result.details.get("progress") != Some(&Value::Bool(true)) {
                            relays.commit();
                            return result;
                        }
                        if let Err(error) = context.send_progress_payload(result.args, result.kwargs).await {
                            return failure(error);
                        }
                        relays.commit();
                    }
                    Ok(None) => return RpcResult::error("wamp.error.canceled"),
                    Err(error) => return failure(error),
                }
            }
        };
        let result = tokio::select! {
            biased;
            _ = stopped.wait_for(|stopped| *stopped) => RpcResult::error("wamp.error.canceled"),
            _ = context.cancelled() => RpcResult::error("wamp.error.canceled"),
            result = results => result,
        };
        if !call.is_finished() {
            call.cancel();
        }
        result
    }
}

struct Inner {
    source: Session,
    forwarder: Arc<Forwarder>,
    registrations: Mutex<Vec<i64>>,
    subscription: Mutex<Option<Subscription>>,
    close_gate: Mutex<()>,
}

impl Inner {
    async fn close(&self) -> Result<(), Error> {
        let _gate = self.close_gate.lock().await;
        self.forwarder.stopped.send_replace(true);
        let calls = std::mem::take(&mut *self.forwarder.calls.lock().await);
        for slot in calls.into_values() {
            if let Some(CallState::Active(call)) = &*slot.lock().await {
                call.rpc.cancel();
            }
        }
        #[cfg(feature = "rpc-streams")]
        {
            let mut streams = self.forwarder.streams.lock().await;
            streams.abort_all();
            while streams.join_next().await.is_some() {}
        }
        let sessions = {
            let mut callers = self.forwarder.callers.lock().unwrap();
            callers.closed = true;
            std::mem::take(&mut callers.sessions)
        };
        for slot in sessions.into_values() {
            if let Some(session) = slot.lock().await.take() {
                let _ = session.close().await;
            }
        }
        let registrations = std::mem::take(&mut *self.registrations.lock().await);
        let subscription = self.subscription.lock().await.take();
        // The router already removed these resources when the source left.
        if !self.source.connected() {
            return Ok(());
        }
        let mut failure = None;
        for id in registrations {
            if let Err(error) = self.source.unregister(id).await {
                failure.get_or_insert(error);
            }
        }
        if let Some(subscription) = subscription
            && let Err(error) = subscription.unsubscribe().await
        {
            failure.get_or_insert(error);
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Owns forwarding registrations and any per-caller backend sessions.
/// Source and shared target sessions remain owned by their callers.
pub struct Bridge {
    inner: Arc<Inner>,
    executor: tokio::runtime::Handle,
}

impl Bridge {
    pub async fn new(
        source: Session,
        target: Session,
        routes: Vec<BridgeRoute>,
        factory: Option<SessionFactory>,
    ) -> Result<Self, Error> {
        let stopped = watch::channel(false).0;
        let forwarder = Arc::new(Forwarder {
            calls: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "rpc-streams")]
            streams: Mutex::new(tokio::task::JoinSet::new()),
            target,
            factory,
            callers: SyncMutex::new(Callers {
                closed: false,
                sessions: HashMap::new(),
            }),
            stopped,
        });
        let bridge = Self {
            inner: Arc::new(Inner {
                source: source.clone(),
                forwarder: forwarder.clone(),
                registrations: Mutex::new(Vec::new()),
                subscription: Mutex::new(None),
                close_gate: Mutex::new(()),
            }),
            executor: tokio::runtime::Handle::current(),
        };
        if forwarder.factory.is_some() {
            let callers = forwarder.clone();
            let subscription = source
                .subscribe_handle(SubscribeRequest::new("wamp.session.on_leave", move |event| {
                    let callers = callers.clone();
                    async move {
                        let Some(id) = event.args.first().and_then(Value::integer) else {
                            return;
                        };
                        let slot = callers.callers.lock().unwrap().sessions.remove(&id);
                        if let Some(slot) = slot
                            && let Some(session) = slot.lock().await.take()
                        {
                            let _ = session.close().await;
                        }
                    }
                }))
                .await?;
            *bridge.inner.subscription.lock().await = Some(subscription);
        }
        for route in routes {
            let prefix = matches!(route.options.get("match"), Some(Value::Str(policy)) if policy == "prefix");
            let procedure = route.procedure.clone();
            let forwarder = forwarder.clone();
            let response = source
                .register_context(route.procedure, route.options, move |context| {
                    let forwarder = forwarder.clone();
                    let procedure = procedure.clone();
                    async move { forwarder.forward(context, procedure, prefix).await }
                })
                .await?;
            if let Some(error) = response.error {
                return Err(Error::application(error));
            }
            bridge.inner.registrations.lock().await.push(response.registration_id);
        }
        let inner = Arc::downgrade(&bridge.inner);
        let mut stopped = forwarder.stopped.subscribe();
        tokio::spawn(async move {
            tokio::select! {
                _ = stopped.wait_for(|stopped| *stopped) => return,
                _ = source.wait_disconnect() => {},
            }
            if let Some(inner) = inner.upgrade() {
                let _ = inner.close().await;
            }
        });
        Ok(bridge)
    }

    pub async fn close(&self) -> Result<(), Error> {
        self.inner.close().await
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.inner.forwarder.stopped.send_replace(true);
        let inner = self.inner.clone();
        self.executor.spawn(async move {
            let _ = inner.close().await;
        });
    }
}

fn failure(error: Error) -> RpcResult {
    RpcResult {
        error: Some(error.application.unwrap_or_else(|| WampError {
            uri: "wamp.error.operation_failed".into(),
            args: Some(vec![Value::Str(error.message)]),
            ..Default::default()
        })),
        ..Default::default()
    }
}
