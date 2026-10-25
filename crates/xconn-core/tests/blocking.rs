#![cfg(feature = "sync")]
use xconn_core::Result as RpcResult;
use xconn_core::{
    sync::{
        client::Client,
        router::{RealmConfig, Router},
        types::RegisterRequest,
    },
    *,
};

#[test]
fn blocking_api_uses_shared_runtime() {
    let router = Router::new();
    router.add_realm("realm", RealmConfig::default()).unwrap();
    let server = router.rawsocket("127.0.0.1:0".parse().unwrap()).unwrap();
    let auth = || {
        Box::new(wampproto::authenticators::anonymous::AnonymousAuthenticator::default())
            as Box<dyn wampproto::authenticators::authenticator::ClientAuthenticator + Send + Sync>
    };
    let a = Client::new(Box::new(JSONSerializerSpec), auth())
        .connect(&format!("rs://{}", server.local_addr()), "realm")
        .unwrap();
    let b = router.connect("realm", Box::new(JSONSerializerSpec), auth()).unwrap();
    let registration = a
        .execute_register(RegisterRequest::new("echo", |inv| RpcResult::new(inv.args, inv.kwargs)))
        .unwrap();
    assert!(registration.error.is_none());
    assert_eq!(
        b.execute_call(CallRequest::new("echo").arg(42)).unwrap().args.unwrap(),
        vec![Value::Int(42)]
    );
    assert!(a.unregister(registration.registration_id).unwrap().is_none());
    a.leave().unwrap();
    b.leave().unwrap();
    router.close();
}

#[test]
fn blocking_progressive_callbacks() {
    let router = Router::new();
    router.add_realm("realm", RealmConfig::default()).unwrap();
    let auth = || {
        Box::new(wampproto::authenticators::anonymous::AnonymousAuthenticator::default())
            as Box<dyn wampproto::authenticators::authenticator::ClientAuthenticator + Send + Sync>
    };
    let a = router.connect("realm", Box::new(JSONSerializerSpec), auth()).unwrap();
    let b = router.connect("realm", Box::new(JSONSerializerSpec), auth()).unwrap();
    a.register_context("progress", std::collections::HashMap::new(), |context| {
        context
            .send_progress(vec![Value::Int(1)], std::collections::HashMap::new())
            .unwrap();
        RpcResult::arg(2)
    })
    .unwrap();
    let mut call = b.call_stream(CallRequest::new("progress")).unwrap();
    assert_eq!(call.next().unwrap().unwrap().args.unwrap(), vec![Value::Int(1)]);
    assert_eq!(call.next().unwrap().unwrap().args.unwrap(), vec![Value::Int(2)]);
    assert!(call.next().unwrap().is_none());
    a.leave().unwrap();
    b.leave().unwrap();
    router.close();
}

#[cfg(feature = "quic")]
#[test]
fn blocking_quic_session() {
    use xconn_core::sync::quic::{Connection, Listener};
    let router = Router::new();
    router.add_realm("realm", RealmConfig::default()).unwrap();
    let listener = Listener::bind(&router, "127.0.0.1:0".parse().unwrap()).unwrap();
    let connection =
        Connection::connect(listener.local_addr(), "localhost", listener.client_config().unwrap()).unwrap();
    let session = connection
        .open_session(
            "realm",
            Box::new(JSONSerializerSpec),
            Box::new(wampproto::authenticators::anonymous::AnonymousAuthenticator::default()),
        )
        .unwrap();
    assert!(session.connected());
    session.leave().unwrap();
    connection.close();
    listener.close();
    router.close();
}

#[cfg(feature = "webtransport")]
#[test]
fn blocking_webtransport_session() {
    use xconn_core::sync::webtransport::{Connection, Listener};
    let router = Router::new();
    router.add_realm("realm", RealmConfig::default()).unwrap();
    let listener = Listener::bind(&router, "127.0.0.1:0".parse().unwrap()).unwrap();
    let connection = Connection::connect(
        &format!("https://{}", listener.local_addr()),
        listener.client_config().unwrap(),
    )
    .unwrap();
    let session = connection
        .open_session(
            "realm",
            Box::new(JSONSerializerSpec),
            Box::new(wampproto::authenticators::anonymous::AnonymousAuthenticator::default()),
        )
        .unwrap();
    assert!(session.connected());
    session.leave().unwrap();
    connection.close();
    listener.close();
    router.close();
}

#[test]
fn blocking_owned_stream_and_iterator_cleanup() {
    use std::{collections::HashMap, sync::mpsc, time::Duration};
    let router = Router::new();
    router.add_realm("realm", RealmConfig::default()).unwrap();
    let auth = || {
        Box::new(wampproto::authenticators::anonymous::AnonymousAuthenticator::default())
            as Box<dyn wampproto::authenticators::authenticator::ClientAuthenticator + Send + Sync>
    };
    let callee = router.connect("realm", Box::new(JSONSerializerSpec), auth()).unwrap();
    let caller = router.connect("realm", Box::new(JSONSerializerSpec), auth()).unwrap();
    let (cancelled, seen) = mpsc::channel();
    callee
        .register_context("stream.wait", HashMap::new(), move |context| {
            context.send_progress(vec![Value::Int(1)], HashMap::new()).unwrap();
            context.cancelled();
            cancelled.send(()).unwrap();
            RpcResult::no_result()
        })
        .unwrap();
    let call = caller.call("stream.wait").stream().unwrap();
    let mut results = call.results();
    assert!(results.next().unwrap().unwrap().is_progress());
    drop(results);
    seen.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(call.is_finished());
    callee.leave().unwrap();
    caller.leave().unwrap();
    router.close();
}

#[test]
fn legacy_websocket_close_wakes_readers_and_closes_the_socket() {
    use std::{
        io::Read,
        net::{TcpListener, TcpStream},
        sync::{Arc, mpsc},
        time::Duration,
    };
    use xconn_core::sync::{peer::Peer, websocket::WebSocketPeer};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut remote, _) = listener.accept().unwrap();
    remote.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let peer: Arc<dyn Peer> = Arc::from(WebSocketPeer::try_new(stream, false).unwrap());
    let (finished, observed) = mpsc::channel();
    let reader = peer.clone();
    let worker = std::thread::spawn(move || finished.send(reader.read()).unwrap());
    peer.close().unwrap();
    peer.close().unwrap();
    assert!(observed.recv_timeout(Duration::from_secs(2)).unwrap().is_err());
    assert!(peer.write(vec![1]).is_err());
    assert_eq!(remote.read(&mut [0; 1]).unwrap(), 0);
    worker.join().unwrap();
}

#[test]
fn legacy_session_close_forwards_to_its_transport() {
    use std::{
        io::Read,
        net::{TcpListener, TcpStream},
        time::Duration,
    };
    use xconn_core::sync::{session::Session, websocket::WebSocketPeer};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut remote, _) = listener.accept().unwrap();
    remote.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let session = Session::new(
        SessionDetails::new(1, "realm".into(), String::new(), String::new()),
        WebSocketPeer::try_new(stream, false).unwrap(),
        Box::new(wampproto::serializers::json::JSONSerializer {}),
    );
    session.close().unwrap();
    assert_eq!(remote.read(&mut [0; 1]).unwrap(), 0);
}

#[test]
fn legacy_websocket_workers_exchange_frames_and_observe_remote_close() {
    use std::{
        net::{TcpListener, TcpStream},
        sync::mpsc,
        time::Duration,
    };
    use tungstenite::{Message, WebSocket, protocol::Role};
    use xconn_core::sync::websocket::WebSocketPeer;

    for binary in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (remote, _) = listener.accept().unwrap();
        remote.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let peer = WebSocketPeer::try_new(stream, binary).unwrap();
        let (finished, received) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let mut socket = WebSocket::from_raw_socket(remote, Role::Server, None);
            let message = socket.read().unwrap();
            assert_eq!(message.is_binary(), binary);
            assert_eq!(message.into_data().as_ref(), b"request");
            socket.send(Message::Text("reply".into())).unwrap();
            // A TCP EOF must also terminate the workers, without a close frame.
        });
        let client = std::thread::spawn(move || {
            peer.write(b"request".to_vec()).unwrap();
            let reply = peer.read().unwrap();
            assert!(peer.read().is_err());
            finished.send(reply).unwrap();
        });
        assert_eq!(received.recv_timeout(Duration::from_secs(3)).unwrap(), b"reply");
        client.join().unwrap();
        server.join().unwrap();
    }
}
