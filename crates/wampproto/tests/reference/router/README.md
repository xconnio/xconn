# Go reference transcripts

`main.go` generates `../../fixtures/router-reference.json` using wampproto-go at
`ecb54c6c2318d7d3184f77ca95cf5f49c779edae`. It runs exact/prefix/wildcard pub/sub,
publisher disclosure, acknowledgements, unsubscribe, and single/first/last/
roundrobin RPC dispatch and results. Rust tests replay the inputs and compare
message fields, recipient selection, and event recipients with these outputs.
Go is needed only to regenerate the fixture, not to run Rust tests.

From this repository root, with the reference checked out in its sibling:

```sh
go -C ../wampproto-go run "$PWD/crates/wampproto/tests/reference/router/main.go" > crates/wampproto/tests/fixtures/router-reference.json
```

Separate Rust regression tests cover deliberate corrections to the reference's
cleanup, cancellation, matching fanout, progressive correlation, and reply
ownership behavior. Random authentication challenges are tested by exchanging
messages with the Rust client, rather than freezing random output in a fixture.
