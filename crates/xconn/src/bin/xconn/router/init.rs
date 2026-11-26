use std::{
    io::{self, Write},
    path::Path,
};
use xconn::wampproto::authenticators::cryptosign::generate_cryptosign_keypair;

use super::{config::Result, listener::generate_loopback_tls_identity};
use crate::keygen::write_file;

pub(super) fn run(config: &Path) -> Result<()> {
    if let Some(parent) = config.parent().filter(|path| !path.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }

    let (public_key, private_key) = generate_cryptosign_keypair()?;
    let (tls_cert, tls_key) = generate_loopback_tls_identity(time::Duration::days(365))?;
    let files = [
        (config.to_owned(), include_str!("config.yaml").to_owned(), 0o600),
        (config.with_file_name("router.key"), format!("{private_key}\n"), 0o600),
        (config.with_file_name("router.pub"), format!("{public_key}\n"), 0o644),
        (config.with_file_name("tls-key.pem"), tls_key.serialize_pem(), 0o600),
        (config.with_file_name("tls-cert.pem"), tls_cert.pem(), 0o644),
    ];

    for (index, (path, contents, mode)) in files.iter().enumerate() {
        if let Err(error) = write_file(path, contents.as_bytes(), *mode) {
            for (created, _, _) in &files[..index] {
                let _ = std::fs::remove_file(created);
            }
            return Err(format!("Could not create {}: {error}", path.display()).into());
        }
    }

    writeln!(io::stdout().lock(), "Router config initialized at {}", config.display())?;
    Ok(())
}
