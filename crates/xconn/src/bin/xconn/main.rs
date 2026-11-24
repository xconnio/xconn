mod router;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "xconn", version, about = "XConn command-line tools")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Configure and run the XConn WAMP router.
    Router {
        /// Path to the router configuration file.
        #[arg(short, long, global = true, default_value = "xconn.yaml")]
        config: PathBuf,
        #[command(subcommand)]
        command: router::Command,
    },
}

fn main() {
    let Command::Router { config, command } = Args::parse().command;
    if let Err(error) = router::run(command, config) {
        eprintln!("xconn router: {error}");
        std::process::exit(1);
    }
}
