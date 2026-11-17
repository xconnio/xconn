//! iOS certificate validation uses Apple's trust store and verifier.
use crate::Error;
use rustls_platform_verifier::BuilderVerifierExt;
use std::sync::Arc;

pub(crate) fn client_config() -> Result<rustls::ClientConfig, Error> {
    rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .and_then(|builder| builder.with_platform_verifier())
        .map(|builder| builder.with_no_client_auth())
        .map_err(|error| Error::new(error.to_string()))
}
