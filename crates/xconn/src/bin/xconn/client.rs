mod transport;

use clap::Args;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    future::ready,
    io::{self, Write},
};
use tokio::{
    signal,
    sync::{mpsc, watch},
};
use xconn::async_::{Result as RpcResult, session::Session};
use xconn::wampproto::authenticators::{
    anonymous::AnonymousAuthenticator, authenticator::ClientAuthenticator, cryptosign::CryptoSignAuthenticator,
    ticket::TicketAuthenticator, wampcra::WAMPCRAAuthenticator,
};
use xconn::{CBORSerializerSpec, JSONSerializerSpec, MsgPackSerializerSpec, SerializerSpec, Value};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Args)]
pub struct Connection {
    /// Router URL.
    #[arg(long, default_value = "ws://localhost:8080/ws")]
    url: String,
    #[command(flatten)]
    trust: transport::Trust,
    /// Realm to join.
    #[arg(long, default_value = "realm1")]
    realm: String,
    /// Authentication ID.
    #[arg(long)]
    authid: Option<String>,
    /// Secret for challenge-response authentication.
    #[arg(long, group = "credentials")]
    secret: Option<String>,
    /// Hex-encoded Ed25519 private key for cryptosign authentication.
    #[arg(long, group = "credentials")]
    private_key: Option<String>,
    /// Read a hex-encoded Ed25519 private key from a file.
    #[arg(long, value_name = "PATH", group = "credentials")]
    private_key_file: Option<std::path::PathBuf>,
    /// Authentication ticket.
    #[arg(long, group = "credentials")]
    ticket: Option<String>,
    /// Message serializer (json, msgpack, or cbor).
    #[arg(long, default_value = "json")]
    serializer: String,
}

impl Connection {
    async fn connect(&self) -> Result<Session> {
        let serializer: Box<dyn SerializerSpec> = match self.serializer.to_ascii_lowercase().as_str() {
            "json" => Box::new(JSONSerializerSpec),
            "cbor" => Box::new(CBORSerializerSpec),
            "msgpack" => Box::new(MsgPackSerializerSpec),
            _ => return Err(format!("Unknown serializer '{}'. Use json, msgpack, or cbor.", self.serializer).into()),
        };
        let authid = self.authid.as_deref().unwrap_or("");
        let extra = HashMap::new();
        let key_from_file = self
            .private_key_file
            .as_ref()
            .map(std::fs::read_to_string)
            .transpose()?;
        let private_key = self
            .private_key
            .as_deref()
            .or_else(|| key_from_file.as_deref().map(str::trim));

        let authenticator: Box<dyn ClientAuthenticator> = if let Some(key) = private_key {
            Box::new(CryptoSignAuthenticator::try_new(authid, key, extra)?)
        } else if let Some(secret) = &self.secret {
            Box::new(WAMPCRAAuthenticator::new(authid, secret, extra))
        } else if let Some(ticket) = &self.ticket {
            Box::new(TicketAuthenticator::new(authid, ticket, extra))
        } else {
            Box::new(AnonymousAuthenticator::new(authid, extra))
        };

        self.trust
            .connect(&self.url, &self.realm, serializer, authenticator)
            .await
    }
}

#[derive(Args)]
pub struct Sessions {
    /// Total number of sessions to open.
    #[arg(long, alias = "parallel", default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    sessions: u32,
    /// Maximum number of active sessions.
    #[arg(long, alias = "concurrency", default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    max_concurrent_sessions: u32,
}

#[derive(Args)]
pub struct Arguments {
    /// Positional arguments. Numbers and booleans are inferred. Use --payload for exact types.
    args: Vec<String>,
    /// Keyword argument. Repeat this flag to supply multiple arguments.
    #[arg(short = 'k', long = "kwarg", value_name = "KEY=VALUE", value_parser = parse_pair)]
    kwargs: Vec<(String, Value)>,
    /// Protocol request option. Repeat this flag to supply multiple options.
    #[arg(short = 'o', long = "option", value_name = "KEY=VALUE", value_parser = parse_pair)]
    options: Vec<(String, Value)>,
    /// JSON object with an optional "args" array and an optional "kwargs" object.
    #[arg(long, value_name = "JSON", conflicts_with_all = ["args", "kwargs"], value_parser = parse_payload)]
    payload: Option<Payload>,
}

impl Arguments {
    pub(super) fn has_option(&self, name: &str) -> bool {
        self.options.iter().any(|(key, _)| key == name)
    }

    fn payload(&self) -> Payload {
        self.payload.clone().unwrap_or_else(|| Payload {
            args: self.args.iter().map(|arg| parse_value(arg)).collect(),
            kwargs: self.kwargs.iter().cloned().collect(),
        })
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    #[serde(default)]
    args: Vec<Value>,
    #[serde(default)]
    kwargs: HashMap<String, Value>,
}

fn parse_payload(input: &str) -> std::result::Result<Payload, String> {
    if !input.trim_start().starts_with('{') {
        return Err("Expected a JSON object with optional args and kwargs fields.".to_owned());
    }
    serde_json::from_str(input).map_err(|error| error.to_string())
}

pub enum Operation<'a> {
    Call(&'a str, &'a Arguments, u32),
    Register(&'a str),
    Publish(&'a str, &'a Arguments, u32, bool),
    Subscribe(&'a str),
}

#[tokio::main]
pub async fn run(connection: Connection, operation: Operation<'_>, sessions: Option<Sessions>) -> Result<()> {
    let sessions = sessions.unwrap_or(Sessions {
        sessions: 1,
        max_concurrent_sessions: 1,
    });
    let listening = matches!(operation, Operation::Register(_) | Operation::Subscribe(_));
    let (shutdown, stop) = watch::channel(false);
    let mut tasks = stream::iter(0..sessions.sessions)
        .take_while(|_| ready(!*stop.borrow()))
        .map(|_| run_session(&connection, &operation, stop.clone()))
        .buffer_unordered(sessions.max_concurrent_sessions as usize);
    let interrupt = signal::ctrl_c();
    tokio::pin!(interrupt);

    let mut result = Ok(());
    let mut stopping = false;
    loop {
        tokio::select! {
            signal = &mut interrupt, if listening && !stopping => {
                result = signal.map_err(Into::into);
                stopping = true;
                shutdown.send_replace(true);
            }
            completed = tasks.next() => {
                let Some(completed) = completed else { break };
                if let Err(error) = completed {
                    if result.is_ok() {
                        result = Err(error);
                    }
                    if listening {
                        stopping = true;
                        shutdown.send_replace(true);
                    }
                }
            }
        }
    }
    result
}

async fn run_session(
    connection: &Connection,
    operation: &Operation<'_>,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    if *stop.borrow() {
        return Ok(());
    }
    let session = tokio::select! {
        result = connection.connect() => result?,
        _ = stop.changed() => return Ok(()),
    };
    let (output_error, mut output_errors) = mpsc::channel(1);
    let result = tokio::select! {
        Some(error) = output_errors.recv() => Err(error.into()),
        result = execute(&session, operation, &output_error) => result,
        _ = stop.changed() => Ok(()),
    };
    let leave = session.leave().await;
    result?;
    leave?;
    Ok(())
}

async fn execute(session: &Session, operation: &Operation<'_>, output_error: &mpsc::Sender<io::Error>) -> Result<()> {
    match *operation {
        Operation::Call(procedure, args, repeat) => {
            for _ in 0..repeat {
                let payload = args.payload();
                let response = session
                    .call(procedure)
                    .args(payload.args)
                    .kwargs(payload.kwargs)
                    .options(args.options.iter().cloned().collect())
                    .execute()
                    .await?;
                if let Some(error) = response.error {
                    return Err(error.uri.into());
                }
                print_payload(&response.args.unwrap_or_default(), &response.kwargs.unwrap_or_default())?;
            }
        }
        Operation::Publish(topic, args, repeat, no_ack) => {
            for _ in 0..repeat {
                let payload = args.payload();
                let request = session
                    .publish(topic)
                    .args(payload.args)
                    .kwargs(payload.kwargs)
                    .options(args.options.iter().cloned().collect())
                    .option("acknowledge", !no_ack);
                if let Some(response) = request.execute().await?
                    && let Some(error) = response.error
                {
                    return Err(error.uri.into());
                }
            }
        }
        Operation::Register(procedure) => {
            let output_error = output_error.clone();
            let response = session
                .register(procedure, move |invocation| {
                    if let Err(error) = print_payload(&invocation.args, &invocation.kwargs) {
                        let _ = output_error.try_send(error);
                    }
                    async move { RpcResult::new(invocation.args, invocation.kwargs) }
                })
                .execute()
                .await?;
            if let Some(error) = response.error {
                return Err(error.uri.into());
            }
            writeln!(
                io::stderr().lock(),
                "Registered procedure '{procedure}'. Press Ctrl+C to exit."
            )?;
            session.wait_disconnect().await;
            return Err("Lost connection to the router.".into());
        }
        Operation::Subscribe(topic) => {
            let output_error = output_error.clone();
            let response = session
                .subscribe(topic, move |event| {
                    if let Err(error) = print_payload(&event.args, &event.kwargs) {
                        let _ = output_error.try_send(error);
                    }
                    ready(())
                })
                .execute()
                .await?;
            if let Some(error) = response.error {
                return Err(error.uri.into());
            }
            writeln!(
                io::stderr().lock(),
                "Subscribed to topic '{topic}'. Press Ctrl+C to exit."
            )?;
            session.wait_disconnect().await;
            return Err("Lost connection to the router.".into());
        }
    }
    Ok(())
}

fn parse_value(input: &str) -> Value {
    if input.len() >= 2
        && ((input.starts_with('\'') && input.ends_with('\'')) || (input.starts_with('"') && input.ends_with('"')))
    {
        return Value::Str(input[1..input.len() - 1].to_owned());
    }
    if let Ok(value) = input.parse::<i64>() {
        return Value::Int(value);
    }
    if let Ok(value) = input.parse::<f64>() {
        return Value::Float(value);
    }
    if let Ok(value) = input.parse::<bool>() {
        return Value::Bool(value);
    }
    Value::Str(input.to_owned())
}

fn parse_pair(input: &str) -> std::result::Result<(String, Value), &'static str> {
    match input.split_once('=') {
        Some((key, value)) if !key.is_empty() => Ok((key.to_owned(), parse_value(value))),
        _ => Err("Expected KEY=VALUE with a nonempty key."),
    }
}

fn print_payload(args: &[Value], kwargs: &HashMap<String, Value>) -> io::Result<()> {
    #[derive(Serialize)]
    struct Output<'a> {
        args: &'a [Value],
        kwargs: &'a HashMap<String, Value>,
    }

    let json = serde_json::to_string_pretty(&Output { args, kwargs })?;
    writeln!(io::stdout().lock(), "{json}")
}
