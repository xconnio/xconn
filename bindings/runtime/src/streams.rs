//! Transport-neutral byte I/O. Existing quic_* operation names remain ABI aliases.
use crate::{
    ByteArray, Result, Value,
    resources::{Output, Resource as BridgeResource, get_resource},
    values::handle_field,
};
use std::collections::HashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};
use xconn_core::async_::rawsocket::BoxStream;

pub(super) struct Stream {
    read: Mutex<tokio::io::ReadHalf<BoxStream>>,
    write: Mutex<tokio::io::WriteHalf<BoxStream>>,
    closed: tokio::sync::watch::Sender<bool>,
    read_deadline: tokio::sync::watch::Sender<Option<tokio::time::Instant>>,
    write_deadline: tokio::sync::watch::Sender<Option<tokio::time::Instant>>,
}

impl Stream {
    pub(super) fn close(&self) {
        self.closed.send_replace(true);
    }
}

pub(super) fn output(stream: BoxStream, fields: HashMap<String, Value>) -> Output {
    let (read, write) = tokio::io::split(stream);
    let (closed, _) = tokio::sync::watch::channel(false);
    Output::resource(
        BridgeResource::Io(Stream {
            read: Mutex::new(read),
            write: Mutex::new(write),
            closed,
            read_deadline: tokio::sync::watch::channel(None).0,
            write_deadline: tokio::sync::watch::channel(None).0,
        }),
        fields,
    )
}

pub(super) async fn dispatch(method: &str, f: HashMap<String, Value>) -> Result<Output> {
    let plain = |value| Ok(Output::plain(value));
    match method {
        #[cfg(feature = "rpc-streams")]
        "rpc_stream_offer" => {
            let session = crate::resources::get_session(handle_field(&f, "handle")?)?;
            let (reference, stream) = session.session.offer_stream().await.map_err(|e| e.to_string())?;
            Ok(output(stream, HashMap::from([("reference".into(), reference)])))
        }
        #[cfg(feature = "rpc-streams")]
        "rpc_stream_accept" => {
            let session = crate::resources::get_session(handle_field(&f, "handle")?)?;
            let reference = f.get("reference").ok_or("stream reference is required")?;
            let stream = session
                .session
                .open_stream_argument(reference)
                .await
                .map_err(|e| e.to_string())?;
            Ok(output(stream, HashMap::new()))
        }
        "quic_read" | "quic_write" | "quic_finish" | "quic_deadlines" => {
            let resource = get_resource(handle_field(&f, "handle")?)?;
            let BridgeResource::Io(Stream {
                read,
                write,
                closed,
                read_deadline,
                write_deadline,
            }) = resource.as_ref()
            else {
                return Err("expected QUIC stream".into());
            };
            if method == "quic_deadlines" {
                for (key, deadline) in [("read", read_deadline), ("write", write_deadline)] {
                    if let Some(value) = f.get(key) {
                        let timeout = match value {
                            Value::Null => None,
                            Value::Int(timestamp) => Some(*timestamp),
                            Value::UInt(timestamp) => {
                                Some(i64::try_from(*timestamp).map_err(|_| "invalid stream deadline")?)
                            }
                            _ => return Err("expected Unix millisecond deadline or null".into()),
                        };
                        let instant = timeout
                            .map(|timestamp| {
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis();
                                let remaining = u128::try_from(timestamp).unwrap_or(0).saturating_sub(now);
                                tokio::time::Instant::now()
                                    .checked_add(std::time::Duration::from_millis(
                                        u64::try_from(remaining).map_err(|_| "stream deadline is out of range")?,
                                    ))
                                    .ok_or("stream deadline is out of range")
                            })
                            .transpose()?;
                        deadline.send_replace(instant);
                    }
                }
                return plain(Value::Null);
            }
            let deadline = if method == "quic_read" {
                read_deadline
            } else {
                write_deadline
            };
            let mut cancellation = closed.subscribe();
            if *cancellation.borrow() {
                return Err("QUIC stream closed".into());
            }
            let operation = async {
                if method == "quic_read" {
                    let size = usize::try_from(handle_field(&f, "size")?).map_err(|_| "invalid read size")?;
                    if size > 1024 * 1024 {
                        return Err("read size exceeds 1 MiB".into());
                    }
                    let mut bytes = vec![0; size];
                    let count = read.lock().await.read(&mut bytes).await.map_err(|e| e.to_string())?;
                    bytes.truncate(count);
                    plain(Value::Bytes(ByteArray(bytes)))
                } else {
                    let mut writer = write.lock().await;
                    if method == "quic_finish" {
                        writer.shutdown().await.map_err(|e| e.to_string())?;
                    } else {
                        let Some(Value::Bytes(bytes)) = f.get("data") else {
                            return Err("expected stream bytes".into());
                        };
                        let count = writer.write(&bytes.0).await.map_err(|e| e.to_string())?;
                        return plain(Value::UInt(count as u64));
                    }
                    plain(Value::Null)
                }
            };

            tokio::select! {
                biased;
                _ = cancellation.changed() => Err("QUIC stream closed".into()),
                _ = wait_deadline(deadline.subscribe()) => Err("stream deadline exceeded".into()),
                result = operation => result,
            }
        }
        _ => Err(format!("unknown stream operation {method}")),
    }
}

// Updating a deadline changes the timer without restarting or cancelling the I/O.
async fn wait_deadline(mut deadline: tokio::sync::watch::Receiver<Option<tokio::time::Instant>>) {
    loop {
        let current = *deadline.borrow_and_update();
        match current {
            Some(instant) => {
                if instant <= tokio::time::Instant::now() {
                    return;
                }
                tokio::select! {
                    _ = tokio::time::sleep_until(instant) => return,
                    result = deadline.changed() => if result.is_err() { return; },
                }
            }
            None => {
                if deadline.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}
