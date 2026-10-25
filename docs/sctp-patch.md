# Pinned SCTP receive-drain patch

Dependency: `webrtc-sctp` 0.13.0, pinned through the workspace Cargo patch to
[`xconnio/webrtc` commit `b4b91964`](https://github.com/xconnio/webrtc/commit/b4b91964e91973778c3080e0f403561f6765522b).
The patch is based on upstream revision `a1f8f1919235d8452835852e018efd654f2f8366`,
the revision recorded in the published crates.io package.
Upstream: [webrtc-rs/webrtc](https://github.com/webrtc-rs/webrtc).

The workspace also pins `webrtc-util` to this revision because SCTP references
it through a local path in the upstream monorepo. This keeps shared types
consistent across the registry WebRTC crates and the forked SCTP crate.

## Problem and change

A Pion peer can send a final reply and immediately reset the channel. SCTP's
`unregister_stream` marks the stream read-shut before the application task has
read every complete message already in its reassembly queue. The upstream
`Stream::read_sctp` checks that flag first and returns EOF, losing those replies.
This reproduced intermittently during real Dart-to-Pion encrypted file-list
requests, even when the rest of the download connection remained healthy.

`Stream::read_sctp` now drains complete received messages before returning EOF.
It also registers its notification waiter before checking queue/closure state,
so a concurrent reset cannot strand the reader between the check and the wait.
The added stream regression test queues replies, simulates remote reset, and
verifies both replies and then EOF. Other upstream code remains unchanged.

Remove this fork override when the selected upstream SCTP release contains
an equivalent fix, rerunning the regression and WebRTC interoperability tests.
CI checks out the pinned fork revision in a separate job and runs:

```sh
cargo test --locked -p webrtc-sctp --lib remote_reset_drains_received_messages_before_eof
```

Until an upstream release includes this fix, Rust applications consuming a
published XConn crate must apply an equivalent root-level Cargo patch; Cargo
does not propagate dependency workspaces' overrides. Native libraries built
from this repository (including the language bindings) use the patch directly.
The Dart build hook tracks Cargo.lock, so changing the pinned revision triggers
a native rebuild.
