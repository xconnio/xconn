function check(condition, message) {
    if (!condition) throw new Error(message);
}

function deferred() {
    let resolve, reject;
    const promise = new Promise((yes, no) => {
        resolve = yes;
        reject = no;
    });
    return { promise, resolve, reject };
}

async function rejects(action, predicate, message) {
    try {
        await action();
    } catch (error) {
        check(predicate(error), `${message}: unexpected ${String(error)}`);
        return;
    }
    throw new Error(`${message}: operation succeeded`);
}

export async function exerciseEdges(api, uri) {
    const { BaseSession, Session, SessionDetails, JSONSerializer, Client, TicketAuthenticator, ProtocolError } = api;
    const serializer = new JSONSerializer();
    const packet = (wire) => ({ type: () => wire[0], marshal: () => wire });
    // Manual disposal is deterministic and idempotent; reused adapters lazily allocate again.
    serializer.serialize(packet([6, {}, "wamp.close.normal"]));
    const codec = serializer.codec;
    check(codec.__wbg_ptr !== 0, "serializer did not allocate native storage");
    serializer.free();
    serializer.free();
    check(codec.__wbg_ptr === 0, "serializer free retained its native pointer");
    const auth = new api.AnonymousAuthenticator();
    void auth.authExtra;
    const adapter = auth.native;
    auth.free();
    auth.free();
    check(adapter.__wbg_ptr === 0, "authenticator free retained its native pointer");
    void auth.authExtra;
    check(auth.native.__wbg_ptr !== 0, "freed authenticator could not be reused");
    auth.free();
    const maximum = serializer.deserialize(serializer.serialize(packet([48, 2 ** 53, {}, "echo"]))).marshal();
    check(maximum[1] === 2 ** 53, "maximum WAMP ID did not remain a Number");
    class MemoryBase extends BaseSession {
        connected = true;
        sent = [];
        incoming = [];
        readers = [];
        listeners = [];
        responder;
        serializer() {
            return serializer;
        }

        getSessionDetails() {
            return new SessionDetails(1, "realm", "", "anonymous");
        }

        isConnected() {
            return this.connected;
        }

        onDisconnect(callback) {
            this.listeners.push(callback);
        }

        receive() {
            if (this.incoming.length) return Promise.resolve(this.incoming.shift());
            if (!this.connected) return Promise.reject(new ProtocolError("disconnected"));
            const waiter = deferred();
            this.readers.push(waiter);
            return waiter.promise;
        }

        push(wire) {
            const payload = typeof wire === "string" ? wire : serializer.serialize(packet(wire));
            const waiter = this.readers.shift();
            if (waiter) waiter.resolve(payload);
            else this.incoming.push(payload);
        }

        send(payload) {
            const wire = serializer.deserialize(payload).marshal();
            this.sent.push(wire);
            this.responder?.(wire);
        }

        async close() {
            if (!this.connected) return;
            this.connected = false;
            for (const waiter of this.readers.splice(0)) waiter.reject(new ProtocolError("disconnected"));
            for (const callback of this.listeners.splice(0)) await callback("disconnected");
        }
    }
    // Old custom peers implement raw send/receive; message-level helpers need not be overridden.
    {
        const base = new MemoryBase();
        const session = new Session(base, { closeTimeout: 10 });
        base.responder = (wire) => {
            if (wire[0] === 48) base.push([50, wire[1], {}, wire[4]]);
        };
        check(
            (await session.call("echo").args("custom peer").execute()).args[0] === "custom peer",
            "custom base session failed",
        );
        await session.close();
        check(!base.connected, "silent GOODBYE peer was not closed on deadline");
    }
    {
        const base = new MemoryBase();
        const session = new Session(base, { closeTimeout: 10 });
        base.responder = (wire) => {
            if (wire[0] === 48) {
                base.push([50, wire[1], { progress: true }, [1]]);
                base.push([50, wire[1], {}, [2]]);
            }
        };
        await rejects(
            () =>
                session.callProgress("echo", () => {
                    throw undefined;
                }),
            (error) => error === undefined,
            "undefined progress rejection",
        );
        await session.close();
        const observed = deferred();
        const sentinel = new Error("late disconnect listener");
        session.onError(observed.resolve);
        session.onDisconnect(() => {
            throw sentinel;
        });
        check((await observed.promise) === sentinel, "late disconnect listener error was not reported");
    }
    for (const streaming of [false, true]) {
        const base = new MemoryBase();
        const session = new Session(base, { closeTimeout: 10 });
        base.responder = (wire) => {
            if (wire[0] === 48) base.push([50, wire[1], { progress: true }, [1]]);
        };
        await rejects(
            async () => {
                if (streaming) {
                    const call = await session.call("echo").stream({ progressiveResults: false });
                    return await call.receive();
                }
                return await session.call("echo").execute();
            },
            (error) => error instanceof ProtocolError && error.message.includes("unsolicited"),
            "unrequested progressive result",
        );
        check(!session.isConnected(), "unsolicited progress kept the connection alive");
        await session.close();
    }
    for (const malformed of ["[]", "not-json"]) {
        const base = new MemoryBase();
        const session = new Session(base, { closeTimeout: 10 });
        const pending = session.call("echo").execute();
        const failed = rejects(
            () => pending,
            (error) => error instanceof ProtocolError,
            "malformed packet",
        );
        base.push(malformed);
        await failed;
        check(!session.isConnected(), "protocol violation kept connection alive");
        await session.close();
    }
    {
        const base = new MemoryBase();
        const session = new Session(base, { closeTimeout: 10 });
        const pending = session.call("echo").execute();
        const failed = rejects(
            () => pending,
            (error) => error instanceof ProtocolError,
            "wrong acknowledgement type",
        );
        base.push([65, base.sent[0][1], 7]);
        await failed;
        await session.close();
    }
    {
        const base = new MemoryBase();
        const session = new Session(base, { closeTimeout: 10 });
        const delivered = deferred();
        base.responder = (wire) => {
            if (wire[0] === 32) {
                base.push([33, wire[1], 7]);
                base.push([36, 7, 9, {}, ["immediate"]]);
            }
        };
        await session.subscribe("events", (event) => delivered.resolve(event)).execute();
        check((await delivered.promise).args[0] === "immediate", "event immediately after SUBSCRIBED was lost");
        await session.close();
    }
    {
        const base = new MemoryBase();
        const session = new Session(base, { closeTimeout: 10 });
        base.responder = (wire) => {
            if (wire[0] === 48 && wire[3] === "echo") base.push([50, wire[1], {}, ["healthy"]]);
        };
        await rejects(
            () => session.call("hang").timeout(10).execute(),
            (error) => error.name === "TimeoutError",
            "local call deadline",
        );
        check(
            base.sent.some((wire) => wire[0] === 49 && wire[1] === 1),
            "timed-out call did not send CANCEL",
        );
        base.push([50, 1, {}, ["late result"]]);
        check((await session.call("echo").execute()).args[0] === "healthy", "late result corrupted pending calls");
        const controller = new AbortController();
        controller.abort();
        const before = base.sent.length;
        await rejects(
            () => session.call("echo").signal(controller.signal).execute(),
            (error) => error.name === "AbortError",
            "pre-aborted call",
        );
        check(before === base.sent.length, "pre-aborted call sent a packet");
        const pending = session.call("hang").execute();
        const failed = rejects(
            () => pending,
            (error) => error instanceof ProtocolError,
            "disconnect pending call",
        );
        await base.close();
        await failed;
        await session.close();
    }
    for (const subscribeFirst of [true, false]) {
        const base = new MemoryBase();
        const session = new Session(base, { closeTimeout: 10 });
        base.responder = (wire) => {
            if (wire[0] === 32) base.push([33, wire[1], 7]);
        };
        let oldEvents = 0;
        const first = await session
            .subscribe("shared", () => {
                oldEvents++;
            })
            .execute();
        const written = deferred();
        const delivered = deferred();
        if (subscribeFirst) {
            base.responder = (wire) => {
                if (wire[0] === 32) written.resolve(wire);
            };
            const subscribing = session.subscribe("shared", delivered.resolve).execute();
            const request = await written.promise;
            const removing = first.unsubscribe();
            await Promise.resolve();
            check(!base.sent.some((wire) => wire[0] === 34), "unsubscribe overtook pending subscribe");
            base.push([33, request[1], 7]);
            await subscribing;
            await removing;
            check(!base.sent.some((wire) => wire[0] === 34), "shared callback removal unsubscribed new handler");
            base.push([36, 7, 100, {}]);
        } else {
            base.responder = (wire) => {
                if (wire[0] === 34) written.resolve(wire);
                if (wire[0] === 32) base.push([33, wire[1], 8]);
            };
            const removing = first.unsubscribe();
            const request = await written.promise;
            const subscribing = session.subscribe("shared", delivered.resolve).execute();
            await Promise.resolve();
            check(base.sent.filter((wire) => wire[0] === 32).length === 1, "subscribe overtook pending unsubscribe");
            base.push([35, request[1]]);
            await removing;
            await subscribing;
            base.push([36, 8, 100, {}]);
        }
        await delivered.promise;
        check(oldEvents === 0, "removed handler received an event");
        await session.close();
    }
    for (const mode of ["kill", "killnowait"]) {
        const base = new MemoryBase();
        const session = new Session(base, { closeTimeout: 10 });
        const started = deferred();
        const release = deferred();
        const replied = deferred();
        base.responder = (wire) => {
            if (wire[0] === 64) base.push([65, wire[1], 7]);
            if (wire[0] === 8 && wire[1] === 68) replied.resolve(wire);
        };
        await session
            .register("interrupted", () => {
                started.resolve();
                return release.promise;
            })
            .execute();
        base.push([68, 101, 7, {}]);
        await started.promise;
        base.push([69, 101, { mode }]);
        if (mode === "kill") {
            const response = await replied.promise;
            check(response[2] === 101 && response[4] === "wamp.error.canceled", "kill did not acknowledge invocation");
        }
        // Let the handler finish and drain the ordered reader without timing sleeps.
        release.resolve();
        base.responder = (wire) => {
            if (wire[0] === 48) base.push([50, wire[1], {}]);
        };
        await session.call("barrier").execute();
        check(
            base.sent.filter((wire) => wire[0] === 8 && wire[2] === 101).length === (mode === "kill" ? 1 : 0),
            "interrupt generated a missing or duplicate reply",
        );
        check(!base.sent.some((wire) => wire[0] === 70 && wire[1] === 101), "cancelled handler sent YIELD");
        await session.close();
    }
    if (uri) {
        const owned = await new Client({ closeTimeout: 500 }).connect(uri, "realm");
        const nativeCodec = owned.base.serializer().codec;
        check(nativeCodec.__wbg_ptr !== 0, "default serializer was not allocated");
        await owned.close();
        check(nativeCodec.__wbg_ptr === 0, "session retained its owned codec after disconnect");
        const borrowed = new JSONSerializer();
        const borrowing = await new Client({ serializer: borrowed, closeTimeout: 500 }).connect(uri, "realm");
        const borrowedCodec = borrowed.codec;
        await borrowing.close();
        check(borrowedCodec.__wbg_ptr !== 0, "session freed its caller-owned codec");
        borrowed.free();
    }
    if (uri) {
        let authenticated = 0;
        class CustomAuthenticator extends TicketAuthenticator {
            async authenticate(challenge) {
                authenticated++;
                await Promise.resolve();
                return super.authenticate(challenge);
            }
        }
        let encoded = 0,
            decoded = 0;
        class CustomSerializer extends JSONSerializer {
            serialize(message) {
                encoded++;
                return super.serialize(message);
            }

            deserialize(payload) {
                decoded++;
                return super.deserialize(payload);
            }
        }
        const session = await new Client({
            serializer: new CustomSerializer(),
            authenticator: new CustomAuthenticator("interop", "secret", {}),
            connectTimeout: 5000,
            closeTimeout: 500,
        }).connect(uri, "realm");
        try {
            check((await session.call("echo").args("custom").execute()).args[0] === "custom", "custom adapters failed");
        } finally {
            await session.close();
        }
        check(authenticated === 1 && encoded > 1 && decoded > 1, "custom callbacks were bypassed");
        const sentinel = new Error("custom authentication failed");
        const rejecting = {
            authMethod: "ticket",
            authID: "interop",
            authExtra: {},
            authenticate: async () => {
                throw sentinel;
            },
        };
        await rejects(
            () => new Client({ authenticator: rejecting, connectTimeout: 5000 }).connect(uri, "realm"),
            (error) => error === sentinel,
            "custom authenticator failure",
        );
        const hanging = { ...rejecting, authenticate: () => new Promise(() => undefined) };
        await rejects(
            () => new Client({ authenticator: hanging, connectTimeout: 30 }).connect(uri, "realm"),
            (error) => error instanceof ProtocolError && error.message.includes("timed out"),
            "handshake deadline",
        );
        const controller = new AbortController();
        controller.abort();
        let opened = false;
        await rejects(
            () =>
                new Client({
                    signal: controller.signal,
                    webSocketFactory: () => {
                        opened = true;
                        throw sentinel;
                    },
                }).connect(uri, "realm"),
            (error) => error.name === "AbortError",
            "pre-aborted connect",
        );
        check(!opened, "pre-aborted connect opened a socket");
    }
    serializer.free();
    return "custom adapters, malformed packets, ACK ordering, ID bounds, cancellation and cleanup";
}

export async function exerciseQueue(SocketInbox) {
    class Socket extends EventTarget {
        readyState = WebSocket.OPEN;
        close() {
            this.readyState = WebSocket.CLOSED;
        }
    }
    const socket = new Socket();
    const inbox = new SocketInbox(socket);
    await inbox.opened;
    const original = TextEncoder.prototype.encode;
    let encodings = 0;
    TextEncoder.prototype.encode = function (value) {
        encodings++;
        return original.call(this, value);
    };
    try {
        const payloads = ["é".repeat(128), "", new Uint8Array([0, 255])];
        for (const payload of payloads) {
            socket.dispatchEvent(new MessageEvent("message", { data: payload }));
        }
        const drained = deferred();
        class Marker extends Blob {
            async arrayBuffer() {
                drained.resolve();
                return new ArrayBuffer(0);
            }
        }
        socket.dispatchEvent(new MessageEvent("message", { data: new Marker() }));
        await drained.promise;
        check(encodings === 2, "queued text was not measured once per frame");
        for (const expected of payloads) {
            check((await inbox.receive()) === expected, "queue changed frame order or content");
        }
        check(encodings === 2, "dequeue encoded queued text again");
    } finally {
        TextEncoder.prototype.encode = original;
        inbox.stop(new Error("test complete"));
    }
    return "queued UTF-8 and binary frames retain their measured sizes";
}
