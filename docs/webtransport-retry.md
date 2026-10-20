# WebTransport Retry limitation

The WebTransport backend currently uses Quion 0.2.2. Its Retry handling fails
when a peer sends a fragmented ClientHello: subsequent Initial packets can be
mistaken for token replays before an existing pending handshake is checked.

Normal builds disable WebTransport Retry and retain connection, pending-handshake,
and endpoint-memory limits. Native Quinn QUIC is separate.

`crates/xconn-core/tests/retry.rs` preserves the ignored manual regression for
an external Go peer. Enable Retry only after adopting and validating an upstream
fix. No dependency patches are applied by this workspace.
