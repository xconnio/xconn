use std::io::{self, Write};

pub(super) fn run(output_file: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    use xconn::wampproto::authenticators::cryptosign::generate_cryptosign_keypair;

    let (public_key, private_key) = generate_cryptosign_keypair()?;
    if let Some(name) = output_file {
        let public_path = format!("{name}.pub");
        write_key(&name, &private_key, 0o600)?;
        if let Err(error) = write_key(&public_path, &public_key, 0o644) {
            let _ = std::fs::remove_file(&name);
            return Err(error.into());
        }
        writeln!(
            io::stdout().lock(),
            "Private key written to {name}.\nPublic key written to {public_path}."
        )?;
    } else {
        writeln!(
            io::stdout().lock(),
            "Public key: {public_key}\nPrivate key: {private_key}"
        )?;
    }
    Ok(())
}

fn write_key(path: &str, key: &str, #[cfg_attr(not(unix), allow(unused_variables))] mode: u32) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(mode);
    }

    let mut file = options.open(path)?;
    if let Err(error) = writeln!(file, "{key}") {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(error);
    }
    Ok(())
}
