//! Separate-process Rust endpoint used by the browser streaming integration tests.
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use xconn_core::{
    Error, JSONSerializerSpec, Payload, Result as RpcResult, StreamOptions, Value, WampError,
    async_::{
        router::{RealmConfig, Router},
        server::Server,
        session::Session,
    },
    wampproto::{authenticators::anonymous::AnonymousAuthenticator, serializers::codec::Codec},
};

type Outcome<T> = std::result::Result<T, Error>;

fn check(condition: bool, message: &str) -> Outcome<()> {
    if condition { Ok(()) } else { Err(Error::new(message)) }
}

async fn receive(
    call: &xconn_core::async_::call::Call,
    args: Vec<Value>,
    kwargs: HashMap<String, Value>,
    progress: bool,
) -> Outcome<()> {
    let result = call.receive().await?.ok_or_else(|| Error::new("missing result"))?;
    check(
        result.is_progress() == progress && result.args == Some(args) && result.kwargs.unwrap_or_default() == kwargs,
        "result payload or progress flag changed",
    )
}

async fn verify_browser(session: Session) -> Outcome<()> {
    let call = session
        .call("js.peer.duplex")
        .arg("first")
        .kwarg("marker", "initial")
        .stream_with(StreamOptions {
            progressive_input: true,
            timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .await?;
    receive(
        &call,
        vec!["first".into()],
        HashMap::from([("marker".into(), "initial".into())]),
        true,
    )
    .await?;
    let receiver = call.clone();
    let receiving = tokio::spawn(async move {
        receive(
            &receiver,
            vec!["second".into()],
            HashMap::from([("marker".into(), "middle".into())]),
            true,
        )
        .await
    });
    call.send(
        vec!["second".into()],
        HashMap::from([("marker".into(), "middle".into())]),
    )
    .await?;
    receiving.await.map_err(|error| Error::new(error.to_string()))??;
    call.finish(vec!["last".into()], HashMap::from([("marker".into(), "final".into())]))
        .await?;
    receive(
        &call,
        vec!["last".into()],
        HashMap::from([("marker".into(), "final".into())]),
        true,
    )
    .await?;
    receive(
        &call,
        vec!["done".into()],
        HashMap::from([("complete".into(), true.into())]),
        false,
    )
    .await?;
    check(call.receive().await?.is_none(), "final result delivered twice")?;
    let failure = session.call("js.peer.fail").stream().await?;
    receive(&failure, vec!["before-error".into()], HashMap::new(), true).await?;
    let error = failure
        .receive()
        .await
        .err()
        .and_then(|error| error.application)
        .ok_or_else(|| Error::new("expected application error"))?;
    check(
        error.uri == "example.failure"
            && error.args == Some(vec!["detail".into()])
            && error.kwargs == Some(HashMap::from([("enabled".into(), true.into())])),
        "application error payload changed",
    )?;
    check(failure.receive().await?.is_none(), "application error delivered twice")?;
    for token in ["cancel", "deadline"] {
        let waiting = session
            .call("js.peer.wait")
            .arg(token)
            .stream_with(StreamOptions {
                timeout: Some(Duration::from_millis(if token == "deadline" { 100 } else { 5000 })),
                ..Default::default()
            })
            .await?;
        receive(&waiting, vec!["started".into()], HashMap::new(), true).await?;
        if token == "cancel" {
            waiting.cancel();
        }
        let error = waiting
            .receive()
            .await
            .err()
            .ok_or_else(|| Error::new("expected cancellation or deadline"))?;
        check(
            error
                .message
                .contains(if token == "cancel" { "cancelled" } else { "timed out" }),
            "unexpected call failure",
        )?;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let state = session.call("js.peer.cancelled").arg(token).execute().await?;
                if state.arg_bool(0).unwrap_or(false) {
                    return Ok::<(), Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| Error::new("JS handler did not observe cancellation"))??;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let router = Router::new();
    router.add_realm("realm", RealmConfig::default())?;
    let server = Server::websocket(router.clone(), "127.0.0.1:0".parse()?).await?;
    let session = router
        .connect(
            "realm",
            Box::new(JSONSerializerSpec),
            Box::new(AnonymousAuthenticator::default()),
        )
        .await?;
    session
        .register_context("rust.peer.duplex", HashMap::new(), |context| async move {
            if let Err(error) = context
                .send_progress(context.invocation.args.clone(), context.invocation.kwargs.clone())
                .await
            {
                return RpcResult::error(&error.to_string());
            }
            if context.invocation.details.get("progress") == Some(&Value::Bool(true)) {
                RpcResult::no_result()
            } else {
                RpcResult::new(vec!["done".into()], HashMap::from([("complete".into(), true.into())]))
            }
        })
        .await?;
    session
        .register_context("rust.peer.fail", HashMap::new(), |context| async move {
            let _ = context.send_progress(vec!["before-error".into()], HashMap::new()).await;
            RpcResult {
                error: Some(WampError {
                    uri: "example.failure".into(),
                    args: Some(vec!["detail".into()]),
                    kwargs: Some(HashMap::from([("enabled".into(), true.into())])),
                }),
                ..Default::default()
            }
        })
        .await?;
    let cancelled = Arc::new(Mutex::new(HashSet::new()));
    let state = cancelled.clone();
    session
        .register_context("rust.peer.wait", HashMap::new(), move |context| {
            let state = state.clone();
            async move {
                let token = context.invocation.arg_str(0).unwrap().to_owned();
                let _ = context.send_progress(vec!["started".into()], HashMap::new()).await;
                context.cancelled().await;
                state.lock().unwrap().insert(token);
                RpcResult::no_result()
            }
        })
        .await?;
    session
        .register("rust.peer.cancelled", move |inv| {
            let state = cancelled.clone();
            async move { RpcResult::arg(state.lock().unwrap().contains(inv.arg_str(0).unwrap())) }
        })
        .execute()
        .await?;
    let caller = session.clone();
    session
        .register("rust.peer.verify_js", move |_| {
            let caller = caller.clone();
            async move {
                match tokio::time::timeout(Duration::from_secs(10), verify_browser(caller)).await {
                    Ok(Ok(())) => RpcResult::arg("verified"),
                    Ok(Err(error)) => RpcResult {
                        error: Some(WampError {
                            uri: "interop.failed".into(),
                            args: Some(vec![error.to_string().into()]),
                            kwargs: None,
                        }),
                        ..Default::default()
                    },
                    Err(_) => RpcResult::error("interop.verification_timeout"),
                }
            }
        })
        .execute()
        .await?;
    let bytes = Codec::Json.encode(&HashMap::from([("ws", format!("ws://{}", server.local_addr))]))?;
    let mut stdout = tokio::io::stdout();
    stdout.write_all(&bytes).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await?;
    let mut line = String::new();
    tokio::io::BufReader::new(tokio::io::stdin())
        .read_line(&mut line)
        .await?;
    session.leave().await?;
    server.close();
    router.close();
    Ok(())
}
