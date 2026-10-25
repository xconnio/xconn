# From many implementations to XConn Core

XConn Core brings together years of work on WAMP and connectivity across the
XConnIO projects. Its foundations come from the people who designed, built,
tested, and maintained the Wampproto and XConn implementations in Rust, Go,
Python, JavaScript, and Dart, along with the WebRTC and application work that
put those implementations to use.

That work established the protocol behavior, APIs, interoperability, and
practical experience this codebase builds on. The consolidation carries that
experience forward into a shared implementation, while updating its
architecture and extending its capabilities.

## One core, native language APIs

Maintaining independent protocol and networking implementations across
languages meant repeating fixes, implementing features several times, and
working to keep behavior consistent. XConn Core moves that shared work into
Rust and exposes it through language bindings.

The sans-I/O WAMP engine remains independent of network I/O. The Rust runtime
provides clients, routers, transports, and reusable connectivity services.
Language bindings adapt those capabilities to the conventions of each host:
blocking and async execution, cancellation, errors, and resource ownership.
Browser clients use WebAssembly for shared logic and browser APIs for network
I/O.

The aim is to implement shared behavior once, verify it together, and make it
available through APIs that feel natural in each language.

## Consolidation in the agentic era

AI-assisted development made it practical to undertake this consolidation
across languages and repositories. Agents helped port and modernize existing
code, build bindings, expand tests, investigate interoperability failures, and
review the resulting implementation.

The direction and foundations come from years of human effort. Agentic tools
helped bring those foundations together and accelerate the next stage of the
project. Both contributions belong in its history: relevant commits credit
original contributors alongside the agents involved, and source references
identify earlier implementations that were imported or adapted.

XConn Core is the continuation of that collective work: a common foundation
that contributors can improve together and application developers can build
on across languages and platforms.
