use std::time::Duration;
use xconn_webrtc::{Connection, MTU};

async fn pair() -> (
    Connection,
    Connection,
    std::sync::Arc<xconn_webrtc::DataChannel>,
    std::sync::Arc<xconn_webrtc::DataChannel>,
) {
    let a = Connection::new(vec![]).await.unwrap();
    let b = Connection::new(vec![]).await.unwrap();
    let local = a.open_channel("data").await.unwrap();
    let offer = a.offer().await.unwrap();
    let answer = b.answer(offer).await.unwrap();
    a.set_remote(answer).await.unwrap();
    let remote = b.accept_channel().await.unwrap();
    local.wait_open(Duration::from_secs(10)).await.unwrap();
    remote.wait_open(Duration::from_secs(10)).await.unwrap();
    (a, b, local, remote)
}

#[tokio::test]
async fn real_data_channels_and_multiple_streams() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (a, b, local, remote) = pair().await;
        let payload = vec![42; MTU * 5];
        let expected = payload.clone();
        let writer = tokio::spawn(async move {
            local.write_message(&payload).await.unwrap();
            local
        });
        assert_eq!(remote.read_message().await.unwrap(), expected);
        let local = writer.await.unwrap();
        remote.write_message(&[]).await.unwrap();
        assert!(local.read_message().await.unwrap().is_empty());
        let extra = a.open_channel("file").await.unwrap();
        let other = b.accept_channel().await.unwrap();
        extra.wait_open(Duration::from_secs(5)).await.unwrap();
        extra.send_packet(bytes::Bytes::from_static(b"raw data")).await.unwrap();
        assert_eq!(other.read_packet().await.unwrap().as_ref(), b"raw data");
        local.close().await.unwrap();
        extra
            .send_packet(bytes::Bytes::from_static(b"still alive"))
            .await
            .unwrap();
        assert_eq!(other.read_packet().await.unwrap().as_ref(), b"still alive");
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}
