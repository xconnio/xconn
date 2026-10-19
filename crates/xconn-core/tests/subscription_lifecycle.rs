#![cfg(feature = "async")]
use std::time::Duration;
use tokio::sync::mpsc;
use xconn_core::{
    _SerializerSpec, JSONSerializerSpec, SessionDetails, Value,
    async_::{
        peer::{MemoryPeer, Peer},
        session::{Session, Subscription},
        types::SubscribeRequest,
    },
};

struct Fixture {
    session: Session,
    broker: Box<dyn Peer>,
}
impl Fixture {
    fn new() -> Self {
        let (client, broker) = MemoryPeer::pair(16);
        Self {
            session: Session::new(
                SessionDetails::new(1, "realm".into(), "user".into(), "user".into()),
                client,
                JSONSerializerSpec.serializer(),
            ),
            broker,
        }
    }

    async fn request(&self) -> (i64, i64) {
        let data = self.broker.read().await.unwrap();
        let wire = JSONSerializerSpec.serializer().deserialize(data).unwrap().marshal();
        let (Value::Int(code), Value::Int(id)) = (&wire[0], &wire[1]) else {
            panic!("invalid request")
        };
        (*code, *id)
    }

    async fn reply(&self, wire: String) {
        self.broker.write(wire.into_bytes()).await.unwrap();
    }

    async fn no_request(&self) {
        let read = self.broker.read();
        tokio::pin!(read);
        assert!(
            futures_util::poll!(&mut read).is_pending(),
            "unexpected control request before acknowledgement"
        );
    }

    async fn subscribe(&self, request: SubscribeRequest) -> Subscription {
        let subscribing = self.session.subscribe_handle(request);
        tokio::pin!(subscribing);
        assert!(futures_util::poll!(&mut subscribing).is_pending());
        let (code, id) = self.request().await;
        assert_eq!(code, 32);
        self.reply(format!("[33,{id},10]")).await;
        subscribing.await.unwrap()
    }
}

fn handler(sender: mpsc::UnboundedSender<usize>, tag: usize) -> SubscribeRequest {
    SubscribeRequest::new("topic", move |_| {
        let sender = sender.clone();
        async move {
            sender.send(tag).unwrap();
        }
    })
}

#[tokio::test]
async fn an_in_flight_subscribe_is_installed_before_removing_another_handle() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let fixture = Fixture::new();
        let (sender, mut events) = mpsc::unbounded_channel();
        let first = fixture.subscribe(handler(sender.clone(), 0)).await;
        let session = fixture.session.clone();
        let second = tokio::spawn(async move { session.subscribe_handle(handler(sender, 1)).await });
        let (code, id) = fixture.request().await;
        assert_eq!(code, 32);
        let removing = first.unsubscribe();
        tokio::pin!(removing);
        assert!(futures_util::poll!(&mut removing).is_pending());
        fixture.no_request().await;
        fixture.reply(format!("[33,{id},10]")).await;
        let second = second.await.unwrap().unwrap();
        assert!(removing.await.unwrap().is_none());
        fixture.no_request().await;
        fixture.reply("[36,10,99,{}]".into()).await;
        assert_eq!(events.recv().await, Some(1));
        assert!(events.try_recv().is_err());
        assert_eq!(second.id, 10);
        fixture.session.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_unsubscribe_finishes_before_a_new_subscription_is_sent() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let fixture = Fixture::new();
        let (sender, mut events) = mpsc::unbounded_channel();
        let first = fixture.subscribe(handler(sender.clone(), 0)).await;
        let removing = tokio::spawn(async move { first.unsubscribe().await });
        let (code, id) = fixture.request().await;
        assert_eq!(code, 34);
        removing.abort();
        assert!(removing.await.unwrap_err().is_cancelled());
        let subscribing = fixture.session.subscribe_handle(handler(sender, 1));
        tokio::pin!(subscribing);
        assert!(futures_util::poll!(&mut subscribing).is_pending());
        fixture.no_request().await;
        fixture.reply(format!("[35,{id}]")).await;
        let (code, id) = fixture.request().await;
        assert_eq!(code, 32);
        fixture.reply(format!("[33,{id},11]")).await;
        let second = subscribing.await.unwrap();
        assert_eq!(second.id, 11);
        fixture.reply("[36,11,99,{}]".into()).await;
        assert_eq!(events.recv().await, Some(1));
        assert!(events.try_recv().is_err());
        fixture.session.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn an_abandoned_subscribe_is_unsubscribed_after_its_acknowledgement() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let fixture = Fixture::new();
        let session = fixture.session.clone();
        let subscribing = tokio::spawn(async move {
            session
                .subscribe_handle(SubscribeRequest::new("topic", |_| async {}))
                .await
        });
        let (code, id) = fixture.request().await;
        assert_eq!(code, 32);
        subscribing.abort();
        assert!(subscribing.await.err().unwrap().is_cancelled());
        fixture.reply(format!("[33,{id},10]")).await;
        let (code, id) = fixture.request().await;
        assert_eq!(code, 34);
        fixture.reply(format!("[35,{id}]")).await;
        fixture.session.close().await.unwrap();
    })
    .await
    .unwrap();
}
