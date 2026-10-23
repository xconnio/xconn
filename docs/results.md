# Result payloads

Every client language uses `Result` for a successful RPC payload, both as the
handler's reply and as the caller's received value. Progressive result callbacks
also receive that payload type. There are no separate InvocationResult,
CallResponse or ProgressResult payload classes in the client API.

A Result exposes positional arguments, keyword arguments, received details and
[typed payload accessors](payload.md). Constructors follow native language
conventions; optional list/map fields preserve omission where the language runtime
already supports that distinction.

## Handler replies

Python:

```python
from xconn_core import Result

return Result(args=["hello"], kwargs={"count": 1})
# Variadic convenience constructor:
return Result.create("hello", count=1)
```

JavaScript / TypeScript:

```js
return new Result(["hello"], { count: 1 });
// Positional convenience constructor:
return Result.create("hello");
```

Dart:

```dart
return Result(args: ['hello'], kwargs: {'count': 1});
// Positional-list convenience constructor:
return Result.create(['hello'], {'count': 1});
```

Go:

```go
return &xconn.Result{Args: []any{"hello"}, Kwargs: map[string]any{"count": 1}}
// Variadic positional constructor:
return xconn.NewResult("hello")
```

Rust:

```rust
use xconn_core::Result as RpcResult;
RpcResult::arg("hello")
// Positional and keyword arguments:
RpcResult::new(vec!["hello".into()], std::collections::HashMap::from([("count".into(), 1.into())]))
```

Rust also offers Result::args, Result::kwarg, Result::kwargs and Default.
The local RpcResult alias avoids confusing this payload with std::result::Result;
the public payload type is xconn_core::Result.

## Received values and errors

The naming cleanup preserves the existing error channels:

| Language | Call execution | Application errors |
| --- | --- | --- |
| Python | Result or awaitable Result | ApplicationError exception |
| JS / TS | Promise<Result> | Rejected Promise with ApplicationError |
| Dart | Future<Result> | ApplicationError exception |
| Go | *Result | Result.Err is an error; WAMP errors retain *xconn.Error |
| Rust | std::result::Result<xconn_core::Result, xconn_core::Error> | Payload.error holds WampError; transport/runtime errors use Err |

In Go, NewResultError(uri, args...) constructs an error reply with a native
*xconn.Error stored in Result.Err. Rust Result::error(uri) constructs a WAMP error
reply; Result::no_result() retains the existing progressive-input sentinel.
These error helpers are explicit; a successful payload has no error.

Result details describe received protocol metadata. Use the dedicated progress
APIs for progress replies; metadata is not a portable mechanism for sending
arbitrary application fields. Use kwargs for application data.

A handler can forward a nested call's received Result directly, including the
existing error representation in Go/Rust. Python/JS/Dart naturally propagate
application exceptions when awaiting nested calls.
