# Independent Go interoperability

This directory contains source templates for the original pure-Go reference.
It is not a standalone Go module and does not depend on migrated compatibility
repositories or standalone Wampproto bindings. The builder archives pinned
original revisions from sibling xconn-go, xconn-webrtc-go and wampproto-go Git
repositories, copies the fixture and creates an isolated module.
Working checkout contents are not used or modified.

Run from the workspace root with a destination that does not yet exist:

```sh
python3 tests/go-interop/prepare_reference.py /tmp/xconn-original-reference
ci/test-reference.sh /tmp/xconn-original-reference/xconn-go-reference
```

The builder enforces CGO_ENABLED=0 and rejects Rust binding dependencies. The
fixture's `provenance` command and adjacent manifest record source revisions and
binary SHA-256. `ci/test-bindings.sh` lints this generated original fixture rather
than compiling templates against unrelated migrated working trees.

Tests cover bidirectional WebSocket/RawSocket RPC and authentication across three
codecs, PubSub, progressive inputs/results, cancellation/disconnect cleanup, QUIC,
WebTransport, WebRTC and the standalone Rust router. Python/Dart test clients
also use this fixture. Their packages expose no router/server APIs.

Go's RawSocket unknown-realm behavior and path-glob matching differ from Rust in
explicitly recorded ways; see [independent validation](../../docs/independent-validation.md).
Browser/TURN/non-loopback deployments require separate validation.
