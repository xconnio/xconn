use std::{
    io::{self, Write},
    path::Path,
};
use xconn::wampproto::authenticators::cryptosign::generate_cryptosign_keypair;

pub(super) fn run(output_file: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let (public_key, private_key) = generate_cryptosign_keypair()?;
    if let Some(name) = output_file {
        let private_path = Path::new(&name);
        let public_path = format!("{name}.pub");
        write_file(private_path, format!("{private_key}\n").as_bytes(), 0o600)?;
        if let Err(error) = write_file(Path::new(&public_path), format!("{public_key}\n").as_bytes(), 0o644) {
            let _ = std::fs::remove_file(private_path);
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

pub(super) fn write_file(
    path: &Path,
    contents: &[u8],
    #[cfg_attr(not(unix), allow(unused_variables))] mode: u32,
) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(mode);
    }

    let mut file = options.open(path)?;
    if let Err(error) = file.write_all(contents) {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(error);
    }
    Ok(())
}
