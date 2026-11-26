use super::Result;
use base64::{Engine, engine::general_purpose::STANDARD};
use clap::Args;
use futures_util::{StreamExt, stream::FuturesUnordered};
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use std::{io::BufReader, path::PathBuf, sync::Arc, time::Duration};
use url::Url;
use xconn::{
    SerializerSpec,
    async_::{
        client::Client,
        joiner::{WebSocketJoiner, join},
        quic,
        rawsocket::{BoxStream, client_handshake},
        session::Session,
        webtransport, yamux,
    },
    wampproto::authenticators::authenticator::ClientAuthenticator,
};

#[derive(Args)]
pub(super) struct Trust {
    /// Trust certificates from this PEM file in addition to the system roots.
    #[arg(long, value_name = "PATH", conflicts_with = "cert_hash")]
    ca_cert: Option<PathBuf>,
    /// Verify a WebTransport certificate by its base64 SHA-256 fingerprint.
    #[arg(long, value_name = "BASE64")]
    cert_hash: Option<String>,
}

impl Trust {
    pub(super) async fn connect(
        &self,
        uri: &str,
        realm: &str,
        serializer: Box<dyn SerializerSpec>,
        authenticator: Box<dyn ClientAuthenticator>,
    ) -> Result<Session> {
        let url = Url::parse(uri)?;
        let secure = matches!(url.scheme(), "quic" | "https" | "rss" | "wss" | "yamuxs");
        if self.cert_hash.is_some() && url.scheme() != "https" {
            return Err("--cert-hash requires an https:// WebTransport URL".into());
        }
        if self.ca_cert.is_some() && !secure {
            return Err("--ca-cert requires a TLS router URL".into());
        }

        let tls = (secure && self.cert_hash.is_none())
            .then(|| self.tls_config())
            .transpose()?;
        tokio::time::timeout(
            Duration::from_secs(20),
            self.connect_inner(&url, realm, serializer, authenticator, tls),
        )
        .await
        .map_err(|_| "connection handshake timed out")?
    }

    async fn connect_inner(
        &self,
        url: &Url,
        realm: &str,
        serializer: Box<dyn SerializerSpec>,
        authenticator: Box<dyn ClientAuthenticator>,
        tls: Option<ClientConfig>,
    ) -> Result<Session> {
        if let Some(hash) = &self.cert_hash {
            let hash = STANDARD.decode(hash)?;
            if hash.len() != 32 {
                return Err("--cert-hash must contain a base64-encoded SHA-256 fingerprint".into());
            }
            let config = webtransport::backend::ClientBuilder::new().with_server_certificate_hashes(vec![hash])?;
            let connection = webtransport::Connection::connect(url.as_str(), config).await?;
            return Ok(connection.open_session(realm, serializer, authenticator).await?);
        }

        let Some(tls) = tls else {
            return Ok(Client::new(serializer, authenticator)
                .connect_with_timeout(url.as_str(), realm, None)
                .await?);
        };

        match url.scheme() {
            "quic" => {
                let connection = connect_quic(url, tls).await?;
                Ok(connection.open_session(realm, serializer, authenticator).await?)
            }
            "https" => {
                let connection = webtransport::Connection::connect_with_tls(url.as_str(), tls).await?;
                Ok(connection
                    .connection
                    .open_session(realm, serializer, authenticator)
                    .await?)
            }
            "wss" => {
                let joiner = WebSocketJoiner::new(serializer.clone(), authenticator);
                let (peer, details) = joiner.join_with_tls(url.as_str(), realm, tls).await?;
                Ok(Session::new(details, peer, serializer.serializer()))
            }
            "rss" | "yamuxs" => {
                let stream = connect_tls_stream(url, tls).await?;
                if url.scheme() == "yamuxs" {
                    return Ok(yamux::connect(stream, realm, serializer, authenticator).await?);
                }
                let peer = client_handshake(stream, serializer.serializer_id()).await?;
                let (peer, details) = join(peer, realm, serializer.serializer(), authenticator).await?;
                Ok(Session::new(details, peer, serializer.serializer()))
            }
            _ => unreachable!("TLS schemes checked before connecting"),
        }
    }

    fn tls_config(&self) -> Result<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
        if let Some(path) = &self.ca_cert {
            let mut file = BufReader::new(std::fs::File::open(path)?);
            let mut certificates = rustls_pemfile::certs(&mut file).peekable();
            if certificates.peek().is_none() {
                return Err(format!("No PEM certificates found in {}", path.display()).into());
            }
            for certificate in certificates {
                roots.add(certificate?)?;
            }
        }
        Ok(
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()?
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    }
}

async fn connect_tls_stream(url: &Url, tls: ClientConfig) -> Result<BoxStream> {
    let (host, port) = host_and_port(url)?;
    let name = ServerName::try_from(host.to_owned())?;
    let connector = tokio_rustls::TlsConnector::from(Arc::new(tls));
    let stream = tokio::net::TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true)?;
    Ok(Box::new(connector.connect(name, stream).await?))
}

fn host_and_port(url: &Url) -> Result<(&str, u16)> {
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err("The router URL cannot contain user information or a fragment".into());
    }
    let host = url.host_str().ok_or("The router URL requires a host")?;
    let port = url.port().ok_or("The router URL requires a port")?;
    Ok((host.trim_matches(['[', ']']), port))
}

async fn connect_quic(url: &Url, mut tls: ClientConfig) -> Result<quic::Connection> {
    let (host, port) = host_and_port(url)?;
    tls.alpn_protocols = vec![quic::ALPN.to_vec()];
    let config = quinn::ClientConfig::new(Arc::new(quinn::crypto::rustls::QuicClientConfig::try_from(tls)?));
    let mut attempts: FuturesUnordered<_> = tokio::net::lookup_host((host, port))
        .await?
        .take(8)
        .map(|address| quic::Connection::connect(address, host, config.clone()))
        .collect();
    let mut last_error = None;
    while let Some(result) = attempts.next().await {
        match result {
            Ok(connection) => return Ok(connection),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.ok_or("Router host resolved to no addresses")?.into())
}
