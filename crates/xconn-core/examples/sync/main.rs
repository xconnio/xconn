use xconn_core::sync::client::connect_anonymous;
use xconn_core::sync::types::{Event, Invocation, Result as RpcResult};

fn main() {
    let session = connect_anonymous("ws://127.0.0.1:8080", "realm1").unwrap_or_else(|e| panic!("{e}"));

    fn registration_handler(inv: Invocation) -> RpcResult {
        RpcResult::new(inv.args, inv.kwargs)
    }

    let register_request = session.register("io.xconn.echo", registration_handler);
    match register_request.execute() {
        Ok(response) => println!("{response:?}"),
        Err(e) => println!("{e}"),
    }

    let call_request = session.call("io.xconn.echo").arg(1).kwarg("name", "John");

    match call_request.execute() {
        Ok(response) => println!("{response:?}"),
        Err(e) => println!("{e}"),
    }

    // Each input gets a progress reply; the final input also gets a final result.
    session
        .register_context("io.xconn.process", Default::default(), |context| {
            context.send_progress(context.args.clone(), Default::default()).unwrap();
            if context.details.get("progress") == Some(&true.into()) {
                RpcResult::no_result()
            } else {
                RpcResult::arg("done")
            }
        })
        .unwrap();
    let call = session
        .call("io.xconn.process")
        .arg("first")
        .stream_with(xconn_core::StreamOptions {
            progressive_input: true,
            ..Default::default()
        })
        .unwrap();
    call.finish(vec!["last".into()], Default::default()).unwrap();
    while let Some(result) = call.receive().unwrap() {
        println!("progress={}, args={:?}", result.is_progress(), result.args);
    }

    fn event_handler(event: Event) {
        println!("received event {event:?}")
    }

    let subscribe_request = session.subscribe("io.xconn.event", event_handler);
    match subscribe_request.execute() {
        Ok(response) => println!("{response:?}"),
        Err(e) => println!("{e}"),
    }

    let publish_request = session
        .publish("io.xconn.event")
        .arg("hey there!")
        .option("acknowledge", true);

    match publish_request.execute() {
        Ok(response) => println!("{response:?}"),
        Err(e) => println!("{e}"),
    }

    session.wait_disconnect();
}
