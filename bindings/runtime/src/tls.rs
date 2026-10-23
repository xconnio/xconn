//! Certificate discovery shared by verified native QUIC and WebTransport clients.
use rustls::pki_types::CertificateDer;

pub(super) fn trusted_certificates() -> Vec<CertificateDer<'static>> {
    #[cfg(target_os = "android")]
    if std::env::var_os("SSL_CERT_FILE").is_none() && std::env::var_os("SSL_CERT_DIR").is_none() {
        // Android 14+ updates system roots through Conscrypt's APEX module.
        // Older devices keep them in the system partition. openssl-probe's
        // Android default is Termux-specific and does not discover either store.
        let certificates = android_certificates(&[
            std::path::Path::new("/apex/com.android.conscrypt/cacerts"),
            std::path::Path::new("/system/etc/security/cacerts"),
        ]);
        if !certificates.is_empty() {
            return certificates;
        }
    }

    rustls_native_certs::load_native_certs().certs
}

#[cfg(any(target_os = "android", test))]
fn android_certificates(directories: &[&std::path::Path]) -> Vec<CertificateDer<'static>> {
    for directory in directories {
        let certificates = rustls_native_certs::load_certs_from_paths(None, Some(directory)).certs;
        if !certificates.is_empty() {
            return certificates;
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    struct Stores(PathBuf);

    impl Stores {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let root = std::env::temp_dir().join(format!(
                "xconn-android-roots-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&root).unwrap();
            Self(root)
        }

        fn certificate(&self, name: &str) -> (PathBuf, Vec<u8>) {
            let directory = self.0.join(name);
            std::fs::create_dir_all(&directory).unwrap();
            let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()])
                .unwrap()
                .cert;
            // Android root files use OpenSSL hash names, not a .pem extension.
            std::fs::write(directory.join("01234567.0"), certificate.pem()).unwrap();
            (directory, certificate.der().to_vec())
        }
    }

    impl Drop for Stores {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn android_prefers_the_updated_conscrypt_store() {
        let stores = Stores::new();
        let (apex, certificate) = stores.certificate("apex");
        let (system, _) = stores.certificate("system");
        let loaded = android_certificates(&[&apex, &system]);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_ref(), certificate);
    }

    #[test]
    fn android_falls_back_to_the_legacy_store() {
        let stores = Stores::new();
        let apex = stores.0.join("missing-apex");
        let (system, certificate) = stores.certificate("system");
        let loaded = android_certificates(&[&apex, &system]);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_ref(), certificate);
    }

    #[test]
    fn android_skips_empty_or_invalid_certificate_stores() {
        let stores = Stores::new();
        let apex = stores.0.join("apex");
        std::fs::create_dir_all(&apex).unwrap();
        std::fs::write(apex.join("not-a-certificate.0"), "invalid").unwrap();
        let (system, certificate) = stores.certificate("system");
        let loaded = android_certificates(&[&apex, &system]);
        assert_eq!(loaded[0].as_ref(), certificate);
        assert!(android_certificates(&[&apex]).is_empty());
    }
}
