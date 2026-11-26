use super::{Result, certificate_check::CertificateCheck};
use base64::{Engine, engine::general_purpose::STANDARD};
use clap::Args;
use futures_util::{StreamExt, stream::FuturesUnordered};
use rustls::{CertificateError, ClientConfig, RootCertStore, client::WebPkiServerVerifier, pki_types::ServerName};
use std::{
    io::{BufReader, ErrorKind},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use url::{Position, Url};
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
            .then(|| self.checked_tls())
            .transpose()?;
        let check = tls.as_ref().map(|(_, check)| check.clone());
        let result = tokio::time::timeout(
            Duration::from_secs(20),
            self.connect_inner(&url, realm, serializer, authenticator, tls),
        )
        .await
        .unwrap_or_else(|_| Err("connection handshake timed out".into()));

        result.map_err(|error| connection_error(&url, error, check.and_then(|check| check.take_error())))
    }

    async fn connect_inner(
        &self,
        url: &Url,
        realm: &str,
        serializer: Box<dyn SerializerSpec>,
        authenticator: Box<dyn ClientAuthenticator>,
        tls: Option<(ClientConfig, Arc<CertificateCheck>)>,
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

        let Some((tls, check)) = tls else {
            return Ok(Client::new(serializer, authenticator)
                .connect_with_timeout(url.as_str(), realm, None)
                .await?);
        };

        match url.scheme() {
            "quic" => {
                let connection = connect_quic(url, tls).await?;
                check.take_error();
                Ok(connection.open_session(realm, serializer, authenticator).await?)
            }
            "https" => {
                let result = webtransport::Connection::connect_with_tls(url.as_str(), tls).await;
                // The backend may report a certificate failure as a framing error.
                // A successful handshake or an HTTP error takes precedence over another address's certificate failure.
                if !matches!(&result, Err(error) if error.message == "transport error: FrameEncodingError" || error.message == "connection timed out")
                {
                    check.take_error();
                }
                Ok(result?
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

    fn checked_tls(&self) -> Result<(ClientConfig, Arc<CertificateCheck>)> {
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
        if roots.is_empty() {
            return Err("No trusted certificates are available. Use --ca-cert to supply a trusted certificate.".into());
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone()).build()?;
        let check = Arc::new(CertificateCheck::new(verifier));
        let tls = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(check.clone())
            .with_no_client_auth();
        Ok((tls, check))
    }
}

fn connection_error(
    url: &Url,
    error: Box<dyn std::error::Error>,
    certificate_error: Option<rustls::Error>,
) -> Box<dyn std::error::Error> {
    let address = &url[Position::BeforeHost..Position::AfterPort];
    match certificate_error {
        Some(rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer)) => {
            format!("The certificate for {address} is not trusted. Use --ca-cert <PEM> to trust the issuing certificate or the self-signed server certificate.").into()
        }
        Some(error) => format!("Certificate verification failed for {address}: {error}").into(),
        None => {
            let endpoint = format!("{}://{address}{}", url.scheme(), url.path());
            let detail = error.to_string();
            let detail = detail.strip_prefix("failed to connect: ").unwrap_or(&detail);
            let detail = detail.strip_prefix("IO error: ").unwrap_or(detail);
            let refused = error.downcast_ref::<std::io::Error>().is_some_and(|error| error.kind() == ErrorKind::ConnectionRefused)
                || detail.starts_with("Connection refused (os error ")
                || detail.starts_with("No connection could be made because the target machine actively refused it.");
            let reason = if refused { "connection refused" } else { detail };
            format!("Could not connect to {endpoint}: {reason}").into()
        }
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
