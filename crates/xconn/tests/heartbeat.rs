#![cfg(feature = "async")]

use futures_util::{StreamExt, poll};
use std::time::Duration;
use tokio::io::DuplexStream;
use tokio_tungstenite::WebSocketStream;
use tungstenite::protocol::Role;
use xconn::async_::{peer::Peer, rawsocket::RawSocketPeer, websocket::WebSocketPeer};

async fn peer(transport: &str, stream: DuplexStream) -> Box<dyn Peer> {
    match transport {
        "rawsocket" => RawSocketPeer::from_stream(Box::new(stream), 1024, 1024),
        "websocket" => {
            let socket = WebSocketStream::from_raw_socket(stream, Role::Server, None).await;
            let (writer, reader) = socket.split();
            WebSocketPeer::new(reader, writer, true)
        }
        _ => unreachable!(),
    }
}

async fn assert_blocked_ping_times_out(transport: &str, writer_busy: bool) {
    let (stream, _remote) = tokio::io::duplex(1);
    let peer = peer(transport, stream).await;
    let write = peer.write(vec![0; 32]);
    tokio::pin!(write);

    if writer_busy {
        assert!(poll!(&mut write).is_pending());
    }

    let error = tokio::time::timeout(Duration::from_secs(2), peer.ping(Some(Duration::from_secs(1))))
        .await
        .expect("ping did not enforce its timeout")
        .unwrap_err();
    assert_eq!(error.to_string(), "ping timed out");
}

#[tokio::test(start_paused = true)]
async fn rawsocket_ping_times_out_waiting_for_writer() {
    assert_blocked_ping_times_out("rawsocket", true).await;
}

#[tokio::test(start_paused = true)]
async fn rawsocket_ping_times_out_sending() {
    assert_blocked_ping_times_out("rawsocket", false).await;
}

#[tokio::test(start_paused = true)]
async fn websocket_ping_times_out_waiting_for_writer() {
    assert_blocked_ping_times_out("websocket", true).await;
}

#[tokio::test(start_paused = true)]
async fn websocket_ping_times_out_sending() {
    assert_blocked_ping_times_out("websocket", false).await;
}
