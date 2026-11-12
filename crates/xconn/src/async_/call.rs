//! Owned, bounded RPC streams. Sending inputs never requires the result receiver lock.
use super::session::{CallStream, Session};
use crate::{CallRequest, Error, Result as RpcResult, StreamOptions, Value};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot};

type Completion = std::result::Result<(), Error>;

struct Input {
    args: Vec<Value>,
    kwargs: HashMap<String, Value>,
    more: bool,
    reply: oneshot::Sender<Completion>,
}

struct Receiver {
    values: mpsc::Receiver<RpcResult>,
    terminal_reported: bool,
}

struct Inner {
    input: mpsc::Sender<Input>,
    receiver: tokio::sync::Mutex<Receiver>,
    input_lock: tokio::sync::Mutex<()>,
    input_finished: AtomicBool,
    done: Arc<AtomicBool>,
    cancelled: AtomicBool,
    error: Arc<Mutex<Option<Error>>>,
    task: tokio::task::AbortHandle,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// One active RPC. Clone the handle to send and receive concurrently.
/// Dropping the last handle cancels an unfinished invocation.
#[derive(Clone)]
pub struct Call {
    inner: Arc<Inner>,
}

impl Call {
    #[doc(hidden)]
    pub async fn open(
        session: &Session,
        request: CallRequest,
        options: StreamOptions,
    ) -> std::result::Result<Self, Error> {
        if options.buffer_size == 0 || options.buffer_size > 65_536 {
            return Err(Error::new("result buffer size must be between 1 and 65536"));
        }

        let request = request
            .option("progress", options.progressive_input)
            .option("receive_progress", options.progressive_results);
        let deadline = options
            .timeout
            .filter(|value| !value.is_zero())
            .map(|value| tokio::time::Instant::now() + value);
        let stream = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, session.call_stream(request))
                .await
                .map_err(|_| Error::new("call timed out"))??,
            None => session.call_stream(request).await?,
        };
        let (input, commands) = mpsc::channel(16);
        let (output, receiver) = mpsc::channel(options.buffer_size);
        let done = Arc::new(AtomicBool::new(false));
        let error = Arc::new(Mutex::new(None));
        let task = tokio::spawn(run(stream, commands, output, done.clone(), error.clone(), deadline));
        Ok(Self {
            inner: Arc::new(Inner {
                input,
                receiver: tokio::sync::Mutex::new(Receiver {
                    values: receiver,
                    terminal_reported: false,
                }),
                input_lock: tokio::sync::Mutex::new(()),
                input_finished: AtomicBool::new(!options.progressive_input),
                done,
                cancelled: AtomicBool::new(false),
                error,
                task: task.abort_handle(),
            }),
        })
    }

    /// Receive progress updates followed by exactly one final result, then None.
    /// Only one receive operation may be in flight.
    pub async fn receive(&self) -> std::result::Result<Option<RpcResult>, Error> {
        let mut receiver = self
            .inner
            .receiver
            .try_lock()
            .map_err(|_| Error::new("another receive is already pending"))?;
        if receiver.terminal_reported {
            return Ok(None);
        }

        let value = if self.inner.cancelled.load(Ordering::Acquire) {
            None
        } else {
            receiver.values.recv().await
        };

        if self.inner.cancelled.load(Ordering::Acquire) || value.is_none() {
            receiver.terminal_reported = true;
            return match self.inner.error.lock().unwrap().take() {
                Some(error) => Err(error),
                None => Ok(None),
            };
        }

        let mut value = value.unwrap();

        if let Some(error) = value.error.take() {
            receiver.terminal_reported = true;
            return Err(Error::application(error));
        }

        Ok(Some(value))
    }

    pub async fn send(&self, args: Vec<Value>, kwargs: HashMap<String, Value>) -> Completion {
        self.submit(args, kwargs, true).await
    }

    pub async fn finish(&self, args: Vec<Value>, kwargs: HashMap<String, Value>) -> Completion {
        self.submit(args, kwargs, false).await
    }

    async fn submit(&self, args: Vec<Value>, kwargs: HashMap<String, Value>, more: bool) -> Completion {
        let _guard = self.inner.input_lock.lock().await;

        if self.inner.done.load(Ordering::Acquire) {
            return Err(Error::new("call is finished"));
        }

        if self.inner.input_finished.load(Ordering::Acquire) {
            return Err(Error::new("progressive input is disabled or already finished"));
        }

        let (reply, response) = oneshot::channel();
        self.inner
            .input
            .send(Input {
                args,
                kwargs,
                more,
                reply,
            })
            .await
            .map_err(|_| Error::new("call is finished"))?;
        response.await.map_err(|_| Error::new("call is finished"))??;

        if !more {
            self.inner.input_finished.store(true, Ordering::Release);
        }

        Ok(())
    }

    /// Request killnowait cancellation and wake any receiver. Idempotent.
    pub fn cancel(&self) {
        if self.inner.done.swap(true, Ordering::AcqRel) {
            return;
        }

        self.inner.cancelled.store(true, Ordering::Release);
        *self.inner.error.lock().unwrap() = Some(Error::new("call cancelled"));
        self.inner.task.abort();
    }

    pub fn is_finished(&self) -> bool {
        self.inner.done.load(Ordering::Acquire)
    }

    /// Adapt the receiver to a standard Rust asynchronous stream.
    pub fn results(&self) -> impl futures_util::Stream<Item = std::result::Result<RpcResult, Error>> + Send + 'static {
        futures_util::stream::unfold(ResultsGuard(self.clone()), |guard| async move {
            let call = &guard.0;

            match call.receive().await {
                Ok(Some(result)) => Some((Ok(result), guard)),
                Ok(None) => None,
                Err(error) => Some((Err(error), guard)),
            }
        })
    }
}

async fn run(
    mut stream: CallStream,
    mut input: mpsc::Receiver<Input>,
    output: mpsc::Sender<RpcResult>,
    done: Arc<AtomicBool>,
    error: Arc<Mutex<Option<Error>>>,
    deadline: Option<tokio::time::Instant>,
) {
    let timeout = async {
        match deadline {
            Some(deadline) => tokio::time::sleep_until(deadline).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => {
                *error.lock().unwrap() = Some(Error::new("call timed out"));
                break;
            }
            command = input.recv() => {
                let Some(command) = command else {
                    break;
                };
                let result = tokio::select! {
                    _ = &mut timeout => Err(Error::new("call timed out")),
                    result = stream.send_input(command.args, command.kwargs, command.more) => result,
                };

                match result {
                    Ok(()) => {
                        let _ = command.reply.send(Ok(()));
                    }
                    Err(failure) => {
                        let message = failure.to_string();
                        let _ = command.reply.send(Err(failure));
                        *error.lock().unwrap() = Some(Error::new(message));
                        break;
                    }
                }
            }
            response = stream.next() => match response {
                Ok(Some(response)) => {
                    let final_result = response.error.is_some()
                        || response.details.get("progress") != Some(&Value::Bool(true));

                    if final_result {
                        done.store(true, Ordering::Release);
                    }
                    if output.try_send(response).is_err() {
                        *error.lock().unwrap() = Some(Error::new("call result queue overflow"));
                        break;
                    }
                    if final_result {
                        break;
                    }
                }
                Ok(None) => break,
                Err(failure) => {
                    *error.lock().unwrap() = Some(failure);
                    break;
                }
            }
        }
    }

    done.store(true, Ordering::Release);
}

struct ResultsGuard(Call);

impl Drop for ResultsGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
