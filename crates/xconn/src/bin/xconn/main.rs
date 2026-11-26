mod client;
mod keygen;
mod router;

use clap::{Arg, CommandFactory, FromArgMatches, Parser, Subcommand, error::ErrorKind, value_parser};
use clap_complete::{Shell, generate};
use client::{Arguments, Connection, Operation, Sessions};
use std::{
    io::{self, Write},
    path::PathBuf,
};

#[derive(Parser)]
#[command(name = "xconn", version, about = "XConn command-line tools")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Call a procedure and print the result as JSON.
    Call {
        #[command(flatten)]
        connection: Connection,
        #[command(flatten)]
        sessions: Sessions,
        /// Procedure URI.
        procedure: String,
        #[command(flatten)]
        args: Arguments,
        /// Number of calls per session.
        #[arg(long, default_value_t = 1, value_parser = value_parser!(u32).range(1..))]
        repeat: u32,
    },
    /// Register an echo procedure that returns the arguments it receives.
    ///
    /// Print invocation arguments as JSON and run until interrupted or disconnected.
    Register {
        #[command(flatten)]
        connection: Connection,
        /// Procedure URI.
        procedure: String,
    },
    /// Publish an event to a topic.
    ///
    /// Wait for the broker to acknowledge the publication unless --no-ack is set.
    Publish {
        #[command(flatten)]
        connection: Connection,
        #[command(flatten)]
        sessions: Sessions,
        /// Topic URI.
        topic: String,
        #[command(flatten)]
        args: Arguments,
        /// Number of publications per session.
        #[arg(long, default_value_t = 1, value_parser = value_parser!(u32).range(1..))]
        repeat: u32,
        /// Send the publication without waiting for the broker to acknowledge it.
        #[arg(long)]
        no_ack: bool,
    },
    /// Subscribe to a topic and print event arguments as JSON.
    ///
    /// A subscription occupies a session slot until it ends. Set --max-concurrent-sessions
    /// to the number of simultaneous subscriptions you want to run.
    Subscribe {
        #[command(flatten)]
        connection: Connection,
        #[command(flatten)]
        sessions: Sessions,
        /// Topic URI.
        topic: String,
    },
    /// Generate an Ed25519 key pair for cryptosign authentication.
    ///
    /// Print both keys by default. Use --output-file to write the keys to separate files.
    Keygen {
        /// Write the private key to NAME and the public key to NAME.pub. Omit NAME to use "key".
        #[arg(short = 'O', long, value_name = "NAME", num_args = 0..=1, default_missing_value = "key")]
        output_file: Option<String>,
    },
    /// Configure and run the XConn router.
    Router {
        /// Path to the router configuration file.
        #[arg(short, long, global = true)]
        config: Option<PathBuf>,
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
    let result = match args.command {
        Command::Call {
            connection,
            sessions,
            procedure,
            args,
            repeat,
        } => client::run(connection, Operation::Call(&procedure, &args, repeat), Some(sessions)),
        Command::Register { connection, procedure } => client::run(connection, Operation::Register(&procedure), None),
        Command::Publish {
            connection,
            sessions,
            topic,
            args,
            repeat,
            no_ack,
        } => {
            if args.has_option("acknowledge") {
                Args::command()
                    .error(
                        ErrorKind::ValueValidation,
                        "Use --no-ack to skip broker acknowledgement. The acknowledge option is reserved.",
                    )
                    .exit();
            }
            client::run(
                connection,
                Operation::Publish(&topic, &args, repeat, no_ack),
                Some(sessions),
            )
        }
        Command::Subscribe {
            connection,
            sessions,
            topic,
        } => client::run(connection, Operation::Subscribe(&topic), Some(sessions)),
        Command::Keygen { output_file } => keygen::run(output_file),
        Command::Router { config, command } => {
            if let Err(error) = router::run(command, config) {
                eprintln!("xconn router: {error}");
                std::process::exit(1);
            }
            return;
        }
    };
    if let Err(error) = result {
        let _ = writeln!(io::stderr().lock(), "xconn: {error}");
        std::process::exit(1);
    }
}
