mod config;
mod listener;

use clap::Subcommand;
use config::{Config, Result};
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum Command {
    /// Write a starter configuration.
    Init,
    /// Validate the configuration without opening listeners.
    Validate,
    /// Run the router in the foreground.
    Start,
}

#[tokio::main]
pub async fn run(command: Command, file: PathBuf) -> Result<()> {
    if matches!(command, Command::Init) {
        if let Some(parent) = file.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }

        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);

        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }

        use std::io::Write;
        options.open(&file)?.write_all(include_bytes!("config.yaml"))?;
        println!("Created {}", file.display());
        return Ok(());
    }

    let config = Config::load(&file)?;
    tracing_subscriber::fmt()
        .with_max_level(config.log_level())
        .with_writer(std::io::stderr)
        .try_init()?;
    // Check includes TLS parsing and key/certificate matching, without binding.
    for transport in &config.transports {
        if let Some(tls) = &transport.tls {
            listener::tls_config(tls)?;
        }
    }

    if matches!(command, Command::Validate) {
        println!("Configuration valid");
        return Ok(());
    }

    let router = config.router()?;
    let mut listeners = Vec::new();

    for transport in config.transports {
        let kind = transport.kind.clone();

        match listener::Listener::bind(router.clone(), transport).await {
            Ok(listener) => {
                println!("Listening {kind} {}", listener.address);
                listeners.push(listener);
            }
            Err(error) => {
                router.close();
                return Err(error);
            }
        }
    }

    let shutdown = shutdown_signal()?;
    println!("Ready");
    use std::io::Write;
    std::io::stdout().flush()?;
    shutdown.await?;

    for listener in &listeners {
        listener.stop();
    }

    router.close();

    for listener in &mut listeners {
        listener.finish().await;
    }

    eprintln!("Stopped");
    Ok(())
}

fn shutdown_signal() -> Result<impl std::future::Future<Output = Result<()>>> {
    #[cfg(unix)]
    {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        Ok(async move {
            tokio::select! {
                _ = interrupt.recv() => {}
                _ = terminate.recv() => {}
            };
            Ok(())
        })
    }

    #[cfg(not(unix))]
    {
        Ok(async {
            tokio::signal::ctrl_c().await?;
            Ok(())
        })
    }
}
