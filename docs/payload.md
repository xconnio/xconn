# Payload parsing

Invocation, Event and Result expose the same strict typed accessors for positional
and keyword arguments in each language. Lifecycle methods and metadata remain on
their respective types. Raw argument containers are still available.

| Type | Python suffix | JS suffix | Dart suffix | Go suffix | Rust suffix |
| --- | --- | --- | --- | --- | --- |
| String | `str` | `String` | `String` | `String` | `str` |
| Boolean | `bool` | `Bool` | `Bool` | `Bool` | `bool` |
| Signed 64-bit integer | `int` | `Int64` | `Int` | `Int64` | `int` |
| Unsigned 64-bit integer | `uint` | `UInt64` | `UInt` | `UInt64` | `uint` |
| Float64 | `float` | `Float64` | `Float` | `Float64` | `float` |
| Bytes | `bytes` | `Bytes` | `Bytes` | `Bytes` | `bytes` |
| List | `list` | `List` | `List` | `List` | `list` |
| String-keyed dictionary | `dict` | `Dict` | `Map` | `Dict` | `dict` |

Use `arg`/`kwarg` prefixes with the language's casing. Examples:

```python
name = invocation.arg_str(0)
count = event.kwarg_int_or("count", 0)
```

```js
const name = invocation.argString(0);
const count = event.kwargInt64Or("count", 0n);
```

```dart
final name = invocation.argString(0);
final count = event.kwargIntOr('count', 0);
```

```go
name, err := invocation.ArgString(0)
count := event.KwargInt64Or("count", 0)
```

```rust
use xconn_core::Payload;
let name = invocation.arg_str(0)?;
let count = event.kwarg_int_or("count", 0);
```

## Conversion rules

- Strings and booleans require the corresponding type. There is no implicit
  stringification, truthiness conversion or numeric-string parsing.
- Integer accessors enforce signed/unsigned 64-bit bounds. Floating-point inputs
  must be finite, whole numbers within +/- (2^53 - 1). They are never truncated.
  Use actual integer payloads for larger values: Python int, Go integer types,
  Rust Int/UInt, Dart int/BigInt and JS BigInt.
- JS integer accessors always return BigInt. Dart unsigned accessors return
  BigInt; signed accessors return int. Other languages use native 64-bit integer
  types, or range-checked Python int.
- Float accessors accept numeric values except booleans, and reject non-finite
  values. Converting a large integer to float may round, as with a normal float64
  conversion; use an integer accessor when exactness matters.
- Bytes require a byte buffer (not a string or an ordinary integer list). List
  accessors exclude byte buffers. Dictionaries must have string keys.
- A strict typed accessor reports `missing`, `null`, `type` or `range`. Python,
  JS and Dart expose PayloadError with a `kind` field; Go returns *PayloadError
  with Kind; Rust returns PayloadError::{Missing, Null, Type, Range}.
- Every typed accessor has an `Or`/`_or` counterpart. It returns the supplied
  fallback when the strict accessor fails. Empty strings/containers, false and
  zero remain valid values rather than triggering defaults.

Raw strict lookup is `require_arg` / `requireArg` in Python/JS/Dart, `arg` / `kwarg`
returning Result in Rust, and List.Get / Dict.Get in Go. It preserves explicit null.
Existing optional raw lookup helpers remain available where already provided.
Host struct/dataclass decoding and generic Dart argAt/kwargAt remain convenience
APIs outside this typed conversion contract.

The change intentionally removes Python's old Result string/numeric coercions,
Dart's fractional truncation, and Go's fractional truncation/string-to-bytes
conversion. Invocation/Event access no longer differs from Result access.

The shared acceptance matrix lives in [tests/payload/cases.json](../tests/payload/cases.json).
Each language checks positional/keyword parsing, typed defaults, missing indices
and keys, binary/container distinctions, 64-bit bounds and non-finite numbers.
The JS suite runs in Node and Chromium. Rust aliases Invocation and Event to the
same incoming payload type; one Payload trait also covers Result.
