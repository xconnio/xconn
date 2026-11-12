//! Blocking adapter over the single asynchronous session implementation.
use super::types::{RegisterRequest, SubscribeRequest};
use crate::async_::session::{CallStream as AsyncCallStream, Session as AsyncSession};
use crate::{
    CallRequest, Error, PublishRequest, PublishResponse, RegisterResponse, RpcResult, SessionDetails,
    SubscribeResponse, WampError,
};
use std::sync::OnceLock;

pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("create xconn runtime")
    })
}

pub(crate) fn block_on<F: std::future::Future>(future: F) -> F::Output {
    runtime().block_on(future)
}

#[derive(Clone, Debug)]
pub struct Session {
    inner: AsyncSession,
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

    pub fn register(&self, procedure: &str, handler: super::types::RegisterFn) -> super::requests::RegisterBuilder<'_> {
        super::requests::RegisterBuilder {
            session: self,
            request: RegisterRequest::new(procedure, handler),
        }
    }

    pub fn subscribe(&self, topic: &str, handler: super::types::EventFn) -> super::requests::SubscribeBuilder<'_> {
        super::requests::SubscribeBuilder {
            session: self,
            request: SubscribeRequest::new(topic, handler),
        }
    }

    pub fn from_async(inner: AsyncSession) -> Self {
        Self { inner }
    }

    pub fn new(
        details: SessionDetails,
        peer: Box<dyn super::peer::Peer>,
        serializer: Box<dyn wampproto::serializers::serializer::Serializer>,
    ) -> Self {
        #[derive(Debug)]
        struct Adapter(std::sync::Arc<dyn super::peer::Peer>);

        #[async_trait::async_trait]
        impl crate::async_::peer::Peer for Adapter {
            fn kind(&self) -> usize {
                self.0.kind()
            }

            async fn read(&self) -> Result<Vec<u8>, Error> {
                let peer = self.0.clone();
                tokio::task::spawn_blocking(move || peer.read())
                    .await
                    .map_err(|e| Error::new(e.to_string()))?
            }

            async fn write(&self, data: Vec<u8>) -> Result<(), Error> {
                let peer = self.0.clone();
                tokio::task::spawn_blocking(move || peer.write(data))
                    .await
                    .map_err(|e| Error::new(e.to_string()))?
            }

            async fn close(&self) -> Result<(), Error> {
                let peer = self.0.clone();
                tokio::task::spawn_blocking(move || peer.close())
                    .await
                    .map_err(|e| Error::new(e.to_string()))?
            }
        }

        block_on(async {
            Self::from_async(AsyncSession::new(
                details,
                Box::new(Adapter(std::sync::Arc::from(peer))),
                serializer,
            ))
        })
    }

    #[doc(hidden)]
    pub fn execute_call(&self, request: CallRequest) -> Result<RpcResult, Error> {
        block_on(self.inner.execute_call(request))
    }

    pub fn call_stream(&self, request: CallRequest) -> Result<CallStream, Error> {
        block_on(self.inner.call_stream(request)).map(|inner| CallStream { inner })
    }

    #[doc(hidden)]
    pub fn execute_publish(&self, request: PublishRequest) -> Result<Option<PublishResponse>, Error> {
        block_on(self.inner.execute_publish(request))
    }

    #[doc(hidden)]
    pub fn execute_register(&self, request: RegisterRequest) -> Result<RegisterResponse, Error> {
        let handler = request.callback();
        let request = crate::async_::types::RegisterRequest::new(request.procedure(), move |invocation| async move {
            tokio::task::spawn_blocking(move || handler(invocation))
                .await
                .unwrap_or_else(|_| crate::RpcResult::error("wamp.error.runtime_error"))
        })
        .with_options(request.options().clone());
        block_on(self.inner.execute_register(request))
    }

    #[doc(hidden)]
    pub fn execute_subscribe(&self, request: SubscribeRequest) -> Result<SubscribeResponse, Error> {
        let handler = request.callback();
        let request = crate::async_::types::SubscribeRequest::new(request.topic(), move |event| async move {
            let _ = tokio::task::spawn_blocking(move || handler(event)).await;
        })
        .with_options(request.options().clone());
        block_on(self.inner.execute_subscribe(request))
    }

    pub fn register_context<F>(
        &self,
        procedure: impl Into<String>,
        options: std::collections::HashMap<String, crate::Value>,
        handler: F,
    ) -> Result<RegisterResponse, Error>
    where
        F: Fn(InvocationContext) -> crate::RpcResult + Send + Sync + 'static,
    {
        let handler = std::sync::Arc::new(handler);
        block_on(self.inner.register_context(procedure, options, move |context| {
            let handler = handler.clone();
            async move {
                tokio::task::spawn_blocking(move || handler(InvocationContext { inner: context }))
                    .await
                    .unwrap_or_else(|_| crate::RpcResult::error("wamp.error.runtime_error"))
            }
        }))
    }

    pub fn as_async(&self) -> &AsyncSession {
        &self.inner
    }

    pub fn unregister(&self, id: i64) -> Result<Option<WampError>, Error> {
        block_on(self.inner.unregister(id))
    }

    pub fn unsubscribe(&self, id: i64) -> Result<Option<WampError>, Error> {
        block_on(self.inner.unsubscribe(id))
    }

    pub fn leave(&self) -> Result<(), Error> {
        block_on(self.inner.leave())
    }

    pub fn close(&self) -> Result<(), Error> {
        block_on(self.inner.close())
    }

    pub fn connected(&self) -> bool {
        self.inner.connected()
    }

    pub fn details(&self) -> &SessionDetails {
        self.inner.details()
    }

    pub fn wait_disconnect(&self) {
        block_on(self.inner.wait_disconnect())
    }
}

pub struct CallStream {
    inner: AsyncCallStream,
}

impl CallStream {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<RpcResult>, Error> {
        block_on(self.inner.next())
    }

    pub fn cancel(&self, mode: &str) -> Result<(), Error> {
        block_on(self.inner.cancel(mode))
    }

    pub fn send_input(
        &mut self,
        args: Vec<crate::Value>,
        kwargs: std::collections::HashMap<String, crate::Value>,
        more: bool,
    ) -> Result<(), Error> {
        block_on(self.inner.send_input(args, kwargs, more))
    }
}

#[derive(Clone)]
pub struct InvocationContext {
    inner: crate::async_::session::InvocationContext,
}

impl std::ops::Deref for InvocationContext {
    type Target = crate::Invocation;

    fn deref(&self) -> &crate::Invocation {
        &self.inner.invocation
    }
}

impl InvocationContext {
    pub fn request_id(&self) -> i64 {
        self.inner.request_id
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    pub fn cancelled(&self) {
        block_on(self.inner.cancelled());
    }

    pub fn send_progress(
        &self,
        args: Vec<crate::Value>,
        kwargs: std::collections::HashMap<String, crate::Value>,
    ) -> Result<(), Error> {
        block_on(self.inner.send_progress(args, kwargs))
    }
}
