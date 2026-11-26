mod cert_hash;
mod config;
mod init;
mod listener;
mod logging;
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
    Start {
        /// Override the configured log level for this run.
        #[arg(long, value_name = "LEVEL", value_parser = config::LOG_LEVELS)]
        log_level: Option<String>,
    },
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

    let mut config = Config::load(&file)?;
    // Check includes TLS parsing and key/certificate matching, without binding.
    for transport in &config.transports {
        if let Some(tls) = &transport.tls {
            listener::tls_config(tls)?;
        }
    }

    let Command::Start { log_level } = command else {
        println!("Configuration valid");
        return Ok(());
    };
    let level = log_level
        .as_deref()
        .map(str::parse)
        .transpose()?
        .unwrap_or_else(|| config.log_level());
    logging::init(level)?;

    let config_path = std::path::absolute(&file)?;
    let (router, management_config) = config.router()?;
    let _management = match management_config {
        Some(config) => {
            Some(Management::start_with_provisioning(router.clone(), Some(config), config::provision_realm).await?)
        }
        None => None,
    };
    let source_credentials = config.source_credentials();
    let source = match config.realm_source.take() {
        Some(config) => Some(realm_source::RealmSource::start(router.clone(), config, source_credentials).await?),
        None => None,
    };
    let mut listeners = Vec::new();

    for transport in &config.transports {
        let kind = &transport.kind;
        let address = &transport.address;

        match listener::Listener::bind(router.clone(), transport).await {
            Ok(listener) => {
                let (endpoint, rawsocket_endpoint) = listener.endpoints(transport);
                let transport_name = match kind.as_str() {
                    "websocket" => "WebSocket",
                    "rawsocket" => "RawSocket",
                    "quic" => "QUIC",
                    "webtransport" => "WebTransport",
                    "yamux" => "Yamux",
                    _ => "Universal",
                };
                let message = match &rawsocket_endpoint {
                    Some(rawsocket) => format!("Listening on {endpoint} (WebSocket) and {rawsocket} (RawSocket)"),
                    None if transport.listener == "unix" && transport.tls.is_some() => {
                        format!("Listening on {endpoint} ({transport_name} over TLS)")
                    }
                    None => format!("Listening on {endpoint} ({transport_name})"),
                };
                tracing::info!(
                    target: logging::LIFECYCLE,
                    transport = %kind,
                    endpoint = %endpoint,
                    rawsocket_endpoint = rawsocket_endpoint.as_deref().map(tracing::field::display),
                    "{message}"
                );
                if let Some(address) = listener.cert_hash_address() {
                    let endpoint = format_args!("http://{address}/wt-cert-hash");
                    tracing::info!(
                        target: logging::LIFECYCLE,
                        endpoint = %endpoint,
                        "Serving WebTransport certificate fingerprint at {endpoint}"
                    );
                }
                listeners.push(listener);
            }
            Err(error) => {
                router.close();
                return Err(format!("Could not bind the {kind} listener at {address}: {error}").into());
            }
        }
    }

    let shutdown = shutdown_signal()?;
    tokio::pin!(shutdown);
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
                config.log_realms();
                let path = config_path.display();
                tracing::info!(target: logging::LIFECYCLE, config = %path, "Router ready (config {path})");
                shutdown.await
            }
            Err(error) => Err(error),
        },
    };

    for listener in &listeners {
        listener.stop();
    }

    let has_connected_clients = router.realm_names().iter().any(|realm| {
        router
            .sessions(realm)
            .is_ok_and(|sessions| sessions.iter().any(|session| session.auth_method != "internal"))
    });
    router.close();

    futures_util::future::join_all(
        listeners
            .into_iter()
            .map(|listener| listener.finish(has_connected_clients)),
    )
    .await;

    tracing::info!(target: logging::LIFECYCLE, "Router stopped");
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
