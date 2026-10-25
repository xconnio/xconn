use crate::common::types::{Error, TRANSPORT_WEB_SOCKET, TransportType};
use crate::sync::peer::Peer;
use mio::net::TcpStream as MioTcpStream;
use mio::{Events, Interest, Poll, Token};
use std::fmt::Debug;
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;
use tungstenite::protocol::Role;
use tungstenite::{Bytes, Message, Utf8Bytes, WebSocket};

const CLIENT: Token = Token(0);

#[derive(Debug, Clone)]
pub struct WebSocketPeer {
    kind: TransportType,
    reader: Arc<Mutex<mpsc::Receiver<Message>>>,
    writer: Arc<mpsc::Sender<Message>>,
    binary: bool,
    control: Arc<TransportControl>,
}

#[derive(Debug)]
struct TransportControl {
    stream: Mutex<Option<TcpStream>>,
    closed: AtomicBool,
}

impl TransportControl {
    fn close(&self) -> Result<(), Error> {
        self.closed.store(true, Ordering::Release);

        if let Some(stream) = self.stream.lock().unwrap().take() {
            stream.shutdown(Shutdown::Both).map_err(|e| Error::new(e.to_string()))?;
        }

        Ok(())
    }
}

impl Peer for WebSocketPeer {
    fn kind(&self) -> TransportType {
        self.kind
    }

    fn read(&self) -> Result<Vec<u8>, Error> {
        let reader = self.reader.lock().unwrap();
        let msg = reader.recv().map_err(|e| Error::new(format!("read error: {e}")))?;
        Ok(msg.into_data().to_vec())
    }

    fn close(&self) -> Result<(), Error> {
        self.control.close()
    }

    fn write(&self, data: Vec<u8>) -> Result<(), Error> {
        if self.control.closed.load(Ordering::Acquire) {
            return Err(Error::new("transport closed"));
        }

        if self.binary {
            self.writer
                .send(Message::Binary(Bytes::copy_from_slice(&data)))
                .map_err(|e| Error::new(format!("write error: {e}")))?;
            Ok(())
        } else {
            let as_string = String::from_utf8(data).map_err(|e| Error::new(format!("Not valid UTF-8: {e}")))?;
            self.writer
                .send(Message::Text(Utf8Bytes::from(as_string)))
                .map_err(|e| Error::new(format!("write error: {e}")))?;
            Ok(())
        }
    }
}

impl WebSocketPeer {
    pub fn try_new(stream: TcpStream, binary: bool) -> Result<Box<dyn Peer>, Error> {
        stream.set_nonblocking(true).map_err(|e| Error::new(e.to_string()))?;
        let control = Arc::new(TransportControl {
            stream: Mutex::new(Some(stream.try_clone().map_err(|e| Error::new(e.to_string()))?)),
            closed: AtomicBool::new(false),
        });
        let mut mio_stream = MioTcpStream::from_std(stream);
        let mut poll = Poll::new().map_err(|e| Error::new(format!("poll error: {e}")))?;
        poll.registry()
            .register(&mut mio_stream, CLIENT, Interest::READABLE | Interest::WRITABLE)
            .map_err(|e| Error::new(format!("register error: {e}")))?;

        // Readiness tracking belongs to the Mio stream used for I/O, especially on Windows.
        let ws = WebSocket::from_raw_socket(mio_stream, Role::Client, None);
        let ws_conn = Arc::new(Mutex::new(ws));
        let ws_writer = Arc::clone(&ws_conn);
        let ws_reader = Arc::clone(&ws_conn);

        let (front_writer, background_reader): (mpsc::Sender<Message>, mpsc::Receiver<Message>) = mpsc::channel();
        let (background_writer, front_reader): (mpsc::Sender<Message>, mpsc::Receiver<Message>) = mpsc::channel();

        let mut events = Events::with_capacity(1024);

        let reader_control = control.clone();
        thread::spawn(move || {
            'reading: while !reader_control.closed.load(Ordering::Acquire) {
                if poll.poll(&mut events, Some(Duration::from_millis(100))).is_err() {
                    break;
                }

                for event in events.iter() {
                    if event.token() != CLIENT {
                        continue;
                    }

                    if event.is_writable() {
                        let result = ws_reader.lock().unwrap().flush();
                        if let Err(error) = result
                            && !matches!(error, tungstenite::Error::Io(ref e) if e.kind() == std::io::ErrorKind::WouldBlock)
                        {
                            break 'reading;
                        }
                    }

                    if event.is_readable() {
                        loop {
                            let result = ws_reader.lock().unwrap().read();
                            match result {
                                Ok(Message::Close(_)) => break 'reading,
                                Ok(message) => {
                                    if background_writer.send(message).is_err() {
                                        break 'reading;
                                    }
                                }
                                Err(tungstenite::Error::Io(error))
                                    if error.kind() == std::io::ErrorKind::WouldBlock =>
                                {
                                    break;
                                }
                                Err(_) => break 'reading,
                            }
                        }
                    }
                }
            }

            let _ = reader_control.close();
        });

        let writer_control = control.clone();
        thread::spawn(move || {
            while !writer_control.closed.load(Ordering::Acquire) {
                let message = match background_reader.recv_timeout(Duration::from_millis(100)) {
                    Ok(message) => message,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                };
                let result = ws_writer.lock().unwrap().send(message);
                if let Err(error) = result {
                    // Tungstenite retains buffered bytes until the readable worker flushes.
                    if !matches!(error, tungstenite::Error::Io(ref e) if e.kind() == std::io::ErrorKind::WouldBlock) {
                        break;
                    }
                }
            }

            let _ = writer_control.close();
        });

        Ok(Box::new(Self {
            kind: TRANSPORT_WEB_SOCKET,
            reader: Arc::new(Mutex::new(front_reader)),
            writer: Arc::new(front_writer),
            binary,
            control,
        }))
    }
}
