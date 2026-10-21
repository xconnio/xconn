//! Pending jobs, completion notification and cancellation ownership.
use crate::{
    Result, Value, dispatch, id,
    resources::Output,
    runtime,
    values::{dictionary, string_field},
};
use futures_util::FutureExt;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::Notify;

enum State {
    Pending,
    Done(Result<Output>),
    Cancelled,
}

struct Job {
    state: Mutex<State>,
    notify: Notify,
    abort: Mutex<Option<tokio::task::AbortHandle>>,
    abortable: bool,
    completion: Option<fn(u64)>,
}

fn jobs() -> &'static Mutex<HashMap<u64, Arc<Job>>> {
    static JOBS: OnceLock<Mutex<HashMap<u64, Arc<Job>>>> = OnceLock::new();
    JOBS.get_or_init(Default::default)
}

fn job(handle: u64) -> Result<Arc<Job>> {
    jobs()
        .lock()
        .unwrap()
        .get(&handle)
        .cloned()
        .ok_or("invalid operation".into())
}

pub fn submit(request: Value) -> u64 {
    submit_with_completion(request, None)
}

pub fn submit_with_completion(request: Value, completion: Option<fn(u64)>) -> u64 {
    let abortable = !matches!(
        dictionary(&request)
            .ok()
            .and_then(|f| string_field(&f, "method").ok())
            .as_deref(),
        Some(
            "connect"
                | "connect_local"
                | "router_new"
                | "listen"
                | "register"
                | "subscribe"
                | "call_open"
                | "stream_open"
                | "callbacks_new"
                | "connect_base"
                | "session_new"
                | "session_from_peer"
        )
    );
    let handle = id();
    let job = Arc::new(Job {
        state: Mutex::new(State::Pending),
        notify: Notify::new(),
        abort: Mutex::new(None),
        abortable,
        completion,
    });
    jobs().lock().unwrap().insert(handle, job.clone());
    let task_job = job.clone();
    let task = runtime().spawn(async move {
        let result = std::panic::AssertUnwindSafe(dispatch(request))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err("native operation panicked".into()));
        let mut state = task_job.state.lock().unwrap();

        if matches!(*state, State::Pending) {
            *state = State::Done(result);
        }

        drop(state);
        task_job.notify.notify_waiters();

        if let Some(completion) = task_job.completion {
            completion(handle);
        }
    });
    *job.abort.lock().unwrap() = Some(task.abort_handle());
    handle
}

pub async fn ready(handle: u64) -> Result<()> {
    let job = job(handle)?;

    loop {
        let notified = job.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if !matches!(*job.state.lock().unwrap(), State::Pending) {
            return Ok(());
        }

        notified.await;
    }
}

pub fn take(handle: u64) -> Result<Value> {
    let job = job(handle)?;
    let mut state = job.state.lock().unwrap();

    if matches!(*state, State::Pending) {
        return Err("operation is not ready".into());
    }

    let result = match std::mem::replace(&mut *state, State::Cancelled) {
        State::Done(result) => result.map(Output::take),
        _ => Err("operation cancelled".into()),
    };
    drop(state);
    jobs().lock().unwrap().remove(&handle);
    result
}

pub fn cancel(handle: u64) {
    let job = jobs().lock().unwrap().remove(&handle);

    if let Some(job) = job {
        *job.state.lock().unwrap() = State::Cancelled;

        if let Some(abort) = job.abort.lock().unwrap().as_ref()
            && job.abortable
        {
            abort.abort();
        }

        job.notify.notify_waiters();
    }
}

pub fn execute(request: Value) -> Result<Value> {
    let handle = submit(request);
    runtime().block_on(ready(handle))?;
    take(handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        close,
        resources::{get_resource, get_router},
        values::{handle_field, object_value},
    };

    #[test]
    fn cancelling_an_unclaimed_resource_releases_it() {
        let operation = submit(object_value([
            ("method", Value::Str("router_new".into())),
            ("realm", Value::Str("unclaimed".into())),
        ]));
        runtime().block_on(ready(operation)).unwrap();
        let job = job(operation).unwrap();
        let handle = match &*job.state.lock().unwrap() {
            State::Done(Ok(output)) => handle_field(&dictionary(&output.value).unwrap(), "handle").unwrap(),
            _ => panic!("router creation failed"),
        };
        let router = get_router(handle).unwrap();
        assert!(router.has_realm("unclaimed"));
        cancel(operation);
        assert!(get_resource(handle).is_err());
        assert!(!router.has_realm("unclaimed"));
        assert!(take(operation).is_err());
    }

    #[test]
    fn results_can_only_be_claimed_once_and_invalid_handles_fail() {
        let operation = submit(object_value([
            ("method", Value::Str("router_new".into())),
            ("realm", Value::Str("claimed".into())),
        ]));
        runtime().block_on(ready(operation)).unwrap();
        let value = take(operation).unwrap();
        assert!(take(operation).is_err());
        let handle = handle_field(&dictionary(&value).unwrap(), "handle").unwrap();
        assert!(get_router(handle).unwrap().has_realm("claimed"));
        close(handle);
        close(handle);
        assert!(
            execute(object_value([
                ("method", Value::Str("call".into())),
                ("handle", Value::UInt(handle)),
                ("uri", Value::Str("missing".into()))
            ]))
            .is_err()
        );
    }
}
