# Progressive RPCs in Rust

`execute()` returns the final result. `stream()` starts an RPC and returns an
owned `Call`. Do not append `execute()` to `stream()`.

```rust,no_run
use xconn_core::StreamOptions;

# async fn example(session: &xconn_core::async_::session::Session) -> Result<(), xconn_core::Error> {
let call = session.call("process").arg("first").stream_with(StreamOptions {
    progressive_input: true,
    progressive_results: true,
    ..Default::default()
}).await?;
call.finish(vec!["last".into()], Default::default()).await?;
while let Some(result) = call.receive().await? {
    println!("{:?}: progressive={}", result.args, result.is_progress());
}
# Ok(())
# }
```

The blocking API uses the same methods without `.await`. Runnable examples live
in `crates/xconn-core/examples`.

## Independent directions

Progressive input defaults to disabled; progressive results default to enabled.
Set `progressive_input: true` to send multiple input chunks. Set
`progressive_results: false` to request only the final response. The stream
options override conflicting `progress` and `receive_progress` request options.

The builder's initial arguments are sent immediately. `send` adds an intermediate
input chunk; `finish` sends the final input chunk. A final input may be empty.
Results can continue after `finish`. A peer may also return a final result early,
in which case further sends fail.

`receive` delivers requested progressive results and exactly one final result,
then `None`. `Result::is_progress()` identifies intermediate replies. Application
errors retain their URI, arguments, and keyword arguments in `Error.application`.

## Callee behavior

For an intermediate input, return `Result::no_result()` to keep the invocation
active. Send intermediate replies with `InvocationContext::send_progress`, and
return a normal `Result` to complete the invocation. The callee can inspect the
invocation details to determine whether the caller requested progressive results.

## Ownership and cancellation

The last call handle cancels unfinished work on drop. Call `cancel()` to cancel
explicitly. Cancellation requests WAMP `killnowait`; cancellation and cleanup
are idempotent. Receiving can run concurrently with sending, but only one receive
may be pending. A dropped Rust receive future does not itself cancel the call.

`StreamOptions.timeout` covers startup and result waiting, including the period
after the final input. Zero disables the duration timeout. The result buffer
defaults to 32 frames and accepts sizes from 1 through 65536. Overflow fails and
cancels the call; it does not silently discard results or block unrelated session
traffic. Producers of many chunks should receive concurrently.

## Request IDs

After a caller starts using progressive input, new call IDs must increase for
the session lifetime. Continuations keep the original ID. The dealer discards
late chunks after completion without creating another invocation or sending a
second final reply. It retains one watermark per progressive caller, removed
when the session leaves. Basic-only callers can reuse completed request IDs.

The runtime allocates and sends new call IDs under an ordering guard. An
exhausted or wrapped call ID sequence requires a new session.

Progressive RPCs are separate from multiplexed transport byte streams.

## Language bindings

All bindings use `stream()` as the execution terminal, with independent input
and result controls. Do not append `execute()`. Progressive input is off by
default; progressive results are on. Final-only streams still receive a terminal
success or application error.

| Binding | Default stream | Progressive input with final result only |
| --- | --- | --- |
| Python asyncio | `await request.stream()` | `await request.stream(progressive_input=True, progressive_results=False)` |
| Python blocking | `request.stream()` | `request.stream(progressive_input=True, progressive_results=False)` |
| Go | `request.Stream()` / `request.StreamContext(ctx)` | `request.Stream(xconn.StreamOptions{ProgressiveInput: true, Results: xconn.ResultsFinalOnly})` |
| Dart | `await request.stream()` | `await request.stream(progressiveInput: true, progressiveResults: false)` |
| JavaScript | `await request.stream()` | `await request.stream({progressiveInput: true, progressiveResults: false})` |

Go accepts zero or one variadic `StreamOptions` value. Its zero-value result mode
requests progress; `ResultsFinalOnly` disables it. Enable progressive results
alongside progressive input for a duplex call.

Python supports context managers; Go callers should defer `call.Close()`.
Dart and JS expose async result iteration and explicit `close()` methods. Close
unfinished calls when stopping consumption. Per-language examples and cancellation
rules live in the [binding READMEs](../bindings).
