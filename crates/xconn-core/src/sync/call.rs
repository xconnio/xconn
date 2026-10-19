use crate::{Error, Result as RpcResult, Value};
use std::collections::HashMap;

#[derive(Clone)]
pub struct Call {
    inner: crate::async_::call::Call,
}

impl Call {
    pub(crate) fn new(inner: crate::async_::call::Call) -> Self {
        Self { inner }
    }

    pub fn receive(&self) -> std::result::Result<Option<RpcResult>, Error> {
        super::session::block_on(self.inner.receive())
    }

    pub fn send(&self, args: Vec<Value>, kwargs: HashMap<String, Value>) -> std::result::Result<(), Error> {
        super::session::block_on(self.inner.send(args, kwargs))
    }

    pub fn finish(&self, args: Vec<Value>, kwargs: HashMap<String, Value>) -> std::result::Result<(), Error> {
        super::session::block_on(self.inner.finish(args, kwargs))
    }

    pub fn cancel(&self) {
        self.inner.cancel();
    }

    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    pub fn results(&self) -> impl Iterator<Item = std::result::Result<RpcResult, Error>> + '_ {
        let guard = ResultsGuard(self.clone());
        std::iter::from_fn(move || guard.0.receive().transpose())
    }
}

struct ResultsGuard(Call);

impl Drop for ResultsGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
