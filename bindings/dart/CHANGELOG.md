## Unreleased

- QUIC: `verifyPeerCertificate` lets callers verify the server certificate chain
  themselves, e.g. to pin a self-signed certificate.

## 0.1.0

- Initial Dart and Flutter client binding for XConn Core.
- WAMP RPC and publish/subscribe with authentication and progressive calls/results.
- Direct WebRTC, QUIC, and WebTransport connections.
- Automatically download and verify native SDK 0.1.1 for Android ARM64,
  Linux x86-64/ARM64, and macOS ARM64; no Rust toolchain required.
