use rustls::{
    DigitallySignedStruct, DistinguishedName, Error, SignatureScheme,
    client::{
        WebPkiServerVerifier,
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use std::sync::{Arc, Mutex};

/// Capture certificate failures before a transport maps them to connection errors.
#[derive(Debug)]
pub(super) struct CertificateCheck {
    verifier: Arc<WebPkiServerVerifier>,
    error: Mutex<Option<Error>>,
}

impl CertificateCheck {
    pub(super) fn new(verifier: Arc<WebPkiServerVerifier>) -> Self {
        Self {
            verifier,
            error: Mutex::new(None),
        }
    }

    pub(super) fn take_error(&self) -> Option<Error> {
        self.error.lock().unwrap().take()
    }
}

impl ServerCertVerifier for CertificateCheck {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        self.verifier
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
            .inspect_err(|error| *self.error.lock().unwrap() = Some(error.clone()))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.verifier.verify_tls12_signature(message, cert, signature)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.verifier.verify_tls13_signature(message, cert, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.verifier.supported_verify_schemes()
    }

    fn root_hint_subjects(&self) -> Option<&[DistinguishedName]> {
        self.verifier.root_hint_subjects()
    }
}
