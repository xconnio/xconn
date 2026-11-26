mod cert_hash;
mod config;
mod init;
mod listener;
mod realm_source;
mod rpc_authentication;

use clap::Subcommand;
use config::{Config, Result};
use std::path::{Path, PathBuf};
use xconn::async_::management::Management;

#[derive(Subcommand)]
pub enum Command {
    /// Create a starter config, cryptosign keys, and TLS files.
    Init,
    /// Validate the configuration without opening listeners.
    Validate,
    /// Run the router in the foreground.
    Start,
}

#[tokio::main]
pub async fn run(command: Command, file: Option<PathBuf>) -> Result<()> {
    let initializing = matches!(command, Command::Init);
    let file = match file {
        Some(path) => path,
        None => default_config(initializing)?.to_owned(),
    };

    if initializing {
        return init::run(&file);
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

    let (router, management_config) = config.router()?;
    let _management = match management_config {
        Some(config) => {
            Some(Management::start_with_provisioning(router.clone(), Some(config), config::provision_realm).await?)
        }
        None => None,
    };
    let source_credentials = config.source_credentials();
    let source = match config.realm_source {
        Some(config) => Some(realm_source::RealmSource::start(router.clone(), config, source_credentials).await?),
        None => None,
    };
    let mut listeners = Vec::new();

    for transport in config.transports {
        let kind = transport.kind.clone();

        match listener::Listener::bind(router.clone(), transport).await {
            Ok(listener) => {
                println!("Listening {kind} {}", listener.address());
                if let Some(address) = listener.cert_hash_address() {
                    println!("Listening cert-hash http://{address}/wt-cert-hash");
                }
                listeners.push(listener);
            }
            Err(error) => {
                router.close();
                return Err(error);
            }
        }
    }

    let shutdown = shutdown_signal()?;
    tokio::pin!(shutdown);
    use std::io::Write;
    std::io::stdout().flush()?;
    let startup = async {
        match &source {
            Some(source) => source.load().await,
            None => Ok(()),
        }
    };
    let result = tokio::select! {
        signal = &mut shutdown => signal,
        started = startup => match started {
            Ok(()) => {
                println!("Ready");
                std::io::stdout().flush()?;
                shutdown.await
            }
            Err(error) => Err(error),
        },
    };

    for listener in &listeners {
        listener.stop();
    }

    router.close();

    futures_util::future::join_all(listeners.into_iter().map(listener::Listener::finish)).await;

    eprintln!("Stopped");
    result
}

fn default_config(initializing: bool) -> Result<&'static Path> {
    let nested = Path::new("xconn/xconn.yaml");
    let local = Path::new("xconn.yaml");

    let nested_exists = match nested.try_exists() {
        Err(error) if error.kind() == std::io::ErrorKind::NotADirectory => false,
        result => result?,
    };
    match (nested_exists, local.try_exists()?) {
        (true, true) => Err("Both xconn/xconn.yaml and xconn.yaml exist. Use --config to choose one.".into()),
        (true, false) => Ok(nested),
        (false, true) => Ok(local),
        (false, false) if initializing => Ok(nested),
        (false, false) => Err(
            "No configuration found at xconn/xconn.yaml or xconn.yaml. Run xconn router init or pass --config.".into(),
        ),
    }
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
