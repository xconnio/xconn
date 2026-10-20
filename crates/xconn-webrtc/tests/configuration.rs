use std::time::Duration;

use bytes::Bytes;
use xconn_webrtc::Connection;

async fn forward_candidates(from: Connection, to: Connection) {
    while let Some(candidate) = from.next_candidate().await {
        let complete = candidate.candidate.is_empty();
        to.add_candidate(candidate).await.unwrap();
        if complete {
            break;
        }
    }
}

#[tokio::test]
async fn trickle_connects_without_candidates_in_initial_sdp_and_buffers_early_candidates() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let a = Connection::new(vec![]).await.unwrap();
        let b = Connection::new(vec![]).await.unwrap();
        let local = a.open_raw_channel("file", Default::default()).await.unwrap();
        assert!(a.wait_connected(Duration::from_millis(5)).await.is_err());
        let offer = a.offer_trickle().await.unwrap();
        assert!(!offer.description.sdp.contains("a=candidate:"));
        let candidate = a.next_candidate().await.unwrap();
        assert!(!candidate.candidate.is_empty());
        // Candidates can arrive before the corresponding remote description.
        b.add_candidate(candidate).await.unwrap();
        let answer = b.answer_trickle(offer).await.unwrap();
        assert!(!answer.description.sdp.contains("a=candidate:"));
        let candidate = b.next_candidate().await.unwrap();
        assert!(!candidate.candidate.is_empty());
        a.add_candidate(candidate).await.unwrap();
        a.set_remote(answer).await.unwrap();
        let a_to_b = tokio::spawn(forward_candidates(a.clone(), b.clone()));
        let b_to_a = tokio::spawn(forward_candidates(b.clone(), a.clone()));
        let remote = b.accept_channel().await.unwrap();
        local.wait_open(Duration::from_secs(5)).await.unwrap();
        remote.wait_open(Duration::from_secs(5)).await.unwrap();
        assert_eq!(remote.protocol(), xconn_webrtc::RAW_CHANNEL_PROTOCOL);
        remote.send_packet(Bytes::from_static(b"server first")).await.unwrap();
        assert_eq!(local.read_packet().await.unwrap().as_ref(), b"server first");
        a_to_b.await.unwrap();
        b_to_a.await.unwrap();
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn receive_overflow_closes_the_channel_without_closing_its_connection() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let a = Connection::new(vec![]).await.unwrap();
        let b = Connection::new(vec![]).await.unwrap();
        let sender = a.open_raw_channel("overflow", Default::default()).await.unwrap();
        let answer = b.answer(a.offer().await.unwrap()).await.unwrap();
        a.set_remote(answer).await.unwrap();
        let receiver = b.accept_channel().await.unwrap();
        sender.wait_open(Duration::from_secs(5)).await.unwrap();
        for _ in 0..100 {
            if sender.send_packet(Bytes::from_static(b"unconsumed")).await.is_err() {
                break;
            }
        }
        receiver.closed().await;
        assert_eq!(
            receiver.failure().await.as_deref(),
            Some("WebRTC receive queue overflow")
        );
        loop {
            match receiver.read_packet().await {
                Ok(_) => {}
                Err(error) => {
                    assert!(error.to_string().contains("overflow"));
                    break;
                }
            }
        }
        sender.closed().await;
        let healthy = a.open_raw_channel("healthy", Default::default()).await.unwrap();
        let remote = b.accept_channel().await.unwrap();
        healthy.wait_open(Duration::from_secs(5)).await.unwrap();
        healthy
            .send_packet(Bytes::from_static(b"still connected"))
            .await
            .unwrap();
        assert_eq!(remote.read_packet().await.unwrap().as_ref(), b"still connected");
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}
