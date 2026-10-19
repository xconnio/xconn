#![cfg(feature = "async")]
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xconn_core::async_::rawsocket::{RawSocketPeer, client_handshake};
use xconn_core::wampproto::transports::rawsocket::{self as rs, SerializerID};

#[tokio::test]
async fn fragmented_handshake_frames_and_ping_pong() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (client, mut server) = tokio::io::duplex(64);
        let serving = tokio::spawn(async move {
            let mut request = [0; 4];
            server.read_exact(&mut request).await.unwrap();
            rs::receive_handshake(&request).unwrap();
            let response = rs::send_handshake(&rs::Handshake::new(SerializerID::JSON, 512)).unwrap();
            for byte in response {
                server.write_all(&[byte]).await.unwrap();
                tokio::task::yield_now().await;
            }
            let mut ping = rs::send_message_header(&rs::MessageHeader::new(rs::Message::Ping, 2));
            ping.extend_from_slice(b"hi");
            for byte in ping {
                server.write_all(&[byte]).await.unwrap();
                tokio::task::yield_now().await;
            }
            let mut pong = [0; 6];
            server.read_exact(&mut pong).await.unwrap();
            assert!(matches!(
                rs::receive_message_header(&pong[..4]).unwrap().kind(),
                rs::Message::Pong
            ));
            assert_eq!(&pong[4..], b"hi");
            let mut frame = rs::send_message_header(&rs::MessageHeader::new(rs::Message::Wamp, 3));
            frame.extend_from_slice(b"msg");
            for byte in frame {
                server.write_all(&[byte]).await.unwrap();
                tokio::task::yield_now().await;
            }
        });
        let peer = client_handshake(Box::new(client), SerializerID::JSON).await.unwrap();
        assert!(peer.write(vec![0; 513]).await.is_err());
        assert_eq!(peer.read().await.unwrap(), b"msg");
        serving.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn oversized_incoming_frame_rejected_before_payload_read() {
    let (client, mut server) = tokio::io::duplex(16);
    let peer = RawSocketPeer::from_stream(Box::new(client), 4, 4);
    server
        .write_all(&rs::send_message_header(&rs::MessageHeader::new(rs::Message::Wamp, 5)))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), peer.read())
            .await
            .unwrap()
            .is_err()
    );
}
