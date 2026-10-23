# Fluent request execution

The execution terminal is `execute()` in Python, JavaScript, Dart and Rust, and
`Execute()` in Go. Go also provides `ExecuteContext(ctx)` on call, register,
subscribe and publish builders for context-controlled operations. Request builders configure an operation locally; execution sends it through
the existing session runtime. Do not introduce new `do`, `Do`, or `do_` aliases.

Progressive RPCs use a separate terminal, `stream()`, returning an owned call. See
the [streaming API](streaming.md) for progressive input, results and cancellation.
The fluent entries are `call`, `register`,
`subscribe` and `publish` in every language, with Go capitalization. The former `_request`/`Request` suffixes and
positional executing session methods have been removed:

| Language | Example |
| --- | --- |
| Python blocking | `session.call("echo").arg("hello").execute()` |
| Python async | `await session.call("echo").arg("hello").execute()` |
| Go | `session.Call("echo").Arg("hello").ExecuteContext(ctx)` |
| JavaScript | `await session.call("echo").arg("hello").execute()` |
| Dart | `await session.call("echo").arg("hello").execute()` |
| Rust blocking | `session.call("echo").arg("hello").execute()?` |
| Rust async | `session.call("echo").arg("hello").execute().await?` |

Each session has equivalent register, subscribe and publish builders. Rust and JS
builders delegate to their existing session operations; protocol algorithms are
not duplicated. Rust builders consume themselves on execution. Existing Python
and Go builders allow repeated execution; Dart shares a completion Future. Those
lifecycle differences are not changed by the naming cleanup. Python/Dart implicit
await support is unchanged. Operations formerly executed by direct session
methods now use fluent builders and explicit execution in examples. A future
common API revision should settle execution ownership explicitly rather than introduce that
behavior change through a method rename.

## Payload accessor consistency

Invocation, Event and Result now share strict typed accessors and defaulting
helpers across all languages. See the [payload parsing contract](payload.md) for
names, error kinds, numeric rules and the shared cross-language test matrix.
