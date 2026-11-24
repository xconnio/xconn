mod router;

use clap::{Arg, CommandFactory, FromArgMatches, Parser, Subcommand, value_parser};
use clap_complete::{Shell, generate};
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
    // Clap's Bash generator includes hidden subcommands. Add this helper only to the parser.
    let matches = Args::command()
        .subcommand(
            clap::Command::new("completions")
                .about("Generate a shell completion script on standard output.")
                .hide(true)
                .arg(Arg::new("shell").required(true).value_parser(value_parser!(Shell))),
        )
        .get_matches();
    if let Some(completions) = matches.subcommand_matches("completions") {
        let shell = *completions.get_one::<Shell>("shell").expect("shell is required");
        generate(shell, &mut Args::command(), "xconn", &mut std::io::stdout());
        return;
    }

    let args = Args::from_arg_matches(&matches).unwrap_or_else(|error| error.exit());
    let Command::Router { config, command } = args.command;
    if let Err(error) = router::run(command, config) {
        eprintln!("xconn router: {error}");
        std::process::exit(1);
    }
}
