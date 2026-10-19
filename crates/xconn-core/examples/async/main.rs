use xconn_core::async_::client::connect_anonymous;
use xconn_core::async_::types::{Event, Invocation, Result as RpcResult};

#[tokio::main]
async fn main() {
    let session = connect_anonymous("ws://localhost:8080/ws", "realm1")
        .await
        .unwrap_or_else(|e| panic!("{e}"));

    async fn registration_handler(inv: Invocation) -> RpcResult {
        RpcResult::new(inv.args, inv.kwargs)
    }

    let register_request = session.register("io.xconn.echo", registration_handler);
    match register_request.execute().await {
        Ok(response) => println!("{response:?}"),
        Err(e) => println!("{e}"),
    }

    let call_request = session.call("io.xconn.echo").arg(1).kwarg("name", "John");

    let response = call_request.execute().await.unwrap();
    println!("args={:?}, kwargs={:?}", response.args, response.kwargs);

    // Each input gets a progress reply; the final input also gets a final result.
    session
        .register_context("io.xconn.process", Default::default(), |context| async move {
            context
                .send_progress(context.invocation.args.clone(), Default::default())
                .await
                .unwrap();
            if context.invocation.details.get("progress") == Some(&true.into()) {
                RpcResult::no_result()
            } else {
                RpcResult::arg("done")
            }
        })
        .await
        .unwrap();
    let call = session
        .call("io.xconn.process")
        .arg("first")
        .stream_with(xconn_core::StreamOptions {
            progressive_input: true,
            ..Default::default()
        })
        .await
        .unwrap();
    call.finish(vec!["last".into()], Default::default()).await.unwrap();
    while let Some(result) = call.receive().await.unwrap() {
        println!("progress={}, args={:?}", result.is_progress(), result.args);
    }

    async fn event_handler(event: Event) {
        println!("received event {event:?}")
    }

    let subscribe_request = session.subscribe("io.xconn.event", event_handler);
    match subscribe_request.execute().await {
        Ok(response) => println!("{response:?}"),
        Err(e) => println!("{e}"),
    }

    let publish_request = session
        .publish("io.xconn.event")
        .arg("hey there!")
        .option("acknowledge", true);

    match publish_request.execute().await {
        Ok(response) => println!("{response:?}"),
        Err(e) => println!("{e}"),
    }

    session.wait_disconnect().await;
}
