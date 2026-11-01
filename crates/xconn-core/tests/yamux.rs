#![cfg(feature = "yamux")]

use std::{sync::Arc, time::Duration};
use xconn_core::{
    CBORSerializerSpec, Result as RpcResult,
    async_::{
        router::{RealmConfig, Router},
        server::Server,
        yamux,
    },
    wampproto::authenticators::anonymous::AnonymousAuthenticator,
};

#[tokio::test]
async fn closing_a_session_closes_its_yamux_connection() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let router = Router::new();
        router.add_realm("example", RealmConfig::default()).unwrap();
        let (client, server) = tokio::io::duplex(4096);
        let serving = tokio::spawn(yamux::serve(router.clone(), Box::new(server)));
        let session = yamux::connect(
            Box::new(client),
            "example",
            Box::new(CBORSerializerSpec),
            Box::new(AnonymousAuthenticator::default()),
        )
        .await
        .unwrap();
        session.close().await.unwrap();
        serving.await.unwrap().unwrap();
        // Keeping the closed session alive must not retain the socket.
        assert!(!session.connected());
        router.close();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn tls_yamux_validates_the_server_and_carries_wamp() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let certificate = generated.cert.der().clone();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server_tls = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![certificate.clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(generated.key_pair.serialize_der()).into(),
            )
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).unwrap();
        let client_tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_tls));
        let router = Router::new();
        router.add_realm("example", RealmConfig::default()).unwrap();
        let server = Server::yamux_tls(router.clone(), "127.0.0.1:0".parse().unwrap(), Arc::new(server_tls))
            .await
            .unwrap();
        let socket = tokio::net::TcpStream::connect(server.local_addr).await.unwrap();
        assert!(
            connector
                .connect("wrong.example".try_into().unwrap(), socket)
                .await
                .is_err()
        );
        let connect = || async {
            let socket = tokio::net::TcpStream::connect(server.local_addr).await.unwrap();
            let tls = connector
                .connect("localhost".try_into().unwrap(), socket)
                .await
                .unwrap();
            yamux::connect(
                Box::new(tls),
                "example",
                Box::new(CBORSerializerSpec),
                Box::new(AnonymousAuthenticator::default()),
            )
            .await
            .unwrap()
        };
        let backend = connect().await;
        let registered = backend
            .register("app.echo", |inv| async move { RpcResult::new(inv.args, inv.kwargs) })
            .execute()
            .await
            .unwrap();
        assert!(registered.error.is_none());
        let caller = connect().await;
        let payload = xconn_core::Value::Bytes(xconn_core::wampproto::messages::types::ByteArray(vec![42; 512 * 1024]));
        let result = caller.call("app.echo").arg(payload.clone()).execute().await.unwrap();
        assert!(result.error.is_none());
        assert_eq!(result.args.unwrap(), vec![payload]);
        caller.close().await.unwrap();
        backend.close().await.unwrap();
        server.close();
        router.close();
    })
    .await
    .unwrap();
}
