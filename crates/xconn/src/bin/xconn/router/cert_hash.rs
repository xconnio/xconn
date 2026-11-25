use super::config::Result;
use base64::{Engine, engine::general_purpose::STANDARD};
use http_body_util::Full;
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use sha2::{Digest, Sha256};
use std::{convert::Infallible, net::SocketAddr, time::Duration};
use tokio::{
    net::TcpListener,
    task::{JoinHandle, JoinSet},
};

pub struct CertHashServer {
    pub address: SocketAddr,
    task: JoinHandle<()>,
}

impl CertHashServer {
    pub async fn bind(address: SocketAddr, certificate: &[u8]) -> Result<Self> {
        let socket = TcpListener::bind(address).await?;
        let address = socket.local_addr()?;
        let hash = STANDARD.encode(Sha256::digest(certificate));
        let body = Bytes::from(format!(r#"{{"hash":"{hash}"}}"#));
        let task = tokio::spawn(async move {
            let mut handlers = JoinSet::new();
            loop {
                tokio::select! {
                    Some(_) = handlers.join_next(), if !handlers.is_empty() => {}
                    incoming = socket.accept(), if handlers.len() < 64 => {
                        let (stream, _) = match incoming {
                            Ok(incoming) => incoming,
                            Err(error) => {
                                tracing::error!(%error, "certificate hash listener accept failed");
                                break;
                            }
                        };
                        let body = body.clone();
                        handlers.spawn(async move {
                            let service = service_fn(move |request| {
                                std::future::ready(Ok::<_, Infallible>(response(request, body.clone())))
                            });
                            let _ = tokio::time::timeout(
                                Duration::from_secs(10),
                                http1::Builder::new()
                                    .keep_alive(false)
                                    .header_read_timeout(None)
                                    .serve_connection(TokioIo::new(stream), service),
                            )
                            .await;
                        });
                    }
                }
            }
        });
        Ok(Self { address, task })
    }
}

impl Drop for CertHashServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn response(request: Request<Incoming>, body: Bytes) -> Response<Full<Bytes>> {
    let mut response = Response::builder();
    let body = if request.uri().path() != "/wt-cert-hash" {
        response = response.status(StatusCode::NOT_FOUND);
        Bytes::new()
    } else if !matches!(*request.method(), Method::GET | Method::HEAD) {
        response = response
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header("allow", "GET, HEAD");
        Bytes::new()
    } else {
        response = response
            .header("content-type", "application/json")
            .header("access-control-allow-origin", "*")
            .header("cache-control", "no-store, no-cache, must-revalidate, max-age=0")
            .header("pragma", "no-cache")
            .header("expires", "0");
        body
    };
    response.body(Full::new(body)).expect("valid static response headers")
}
