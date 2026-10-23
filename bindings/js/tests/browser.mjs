// Runs unchanged against the installed browser package in Node and Chromium.
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
        return error;
    }
    throw new Error(`${message}: operation succeeded`);
}

export async function exercise(api, variant, uri) {
    const {
        Client,
        Result,
        ApplicationError,
        JSONSerializer,
        CBORSerializer,
        MsgPackSerializer,
        AnonymousAuthenticator,
        TicketAuthenticator,
        WAMPCRAAuthenticator,
        CryptoSignAuthenticator,
    } = api;
    const serializers =
        variant === "json"
            ? [new JSONSerializer()]
            : [new JSONSerializer(), new CBORSerializer(), new MsgPackSerializer()];
    let authenticatedRpcRoundTrips = 0;
    let sessions = 0;
    const errors = [];
    for (const serializer of serializers) {
        const wire = [48, 42, {}, "echo", ["hello"], { enabled: true }];
        const message = { marshal: () => wire, type: () => 48 };
        const decoded = serializer.deserialize(serializer.serialize(message)).marshal();
        check(decoded[3] === "echo" && decoded[5].enabled, "codec roundtrip failed");
        await rejects(
            () => serializer.deserialize(new Uint8Array([255])),
            () => true,
            "invalid packet accepted",
        );
        if (!(serializer instanceof JSONSerializer)) {
            const payload = [new Uint8Array([0, 255]), 18446744073709551615n, -9223372036854775808n];
            const packet = { marshal: () => [48, 1, {}, "echo", payload], type: () => 48 };
            const roundtrip = serializer.deserialize(serializer.serialize(packet)).marshal()[4];
            check(roundtrip[0] instanceof Uint8Array && roundtrip[0][1] === 255, "binary changed");
            check(roundtrip[1] === payload[1] && roundtrip[2] === payload[2], "64-bit payload changed");
        }
        if (!uri) continue;
        const authenticators = [new AnonymousAuthenticator("", {}), new TicketAuthenticator("interop", "secret", null)];
        if (variant === "full")
            authenticators.push(
                new WAMPCRAAuthenticator("interop", "secret", null),
                new WAMPCRAAuthenticator("salted", "secret", {}),
                new CryptoSignAuthenticator("interop", "11".repeat(32), {}),
            );
        for (const authenticator of authenticators) {
            let announced;
            const session = await new Client({
                serializer,
                authenticator,
                connectTimeout: 5000,
                closeTimeout: 500,
                webSocketFactory: (address, protocols) => {
                    const socket = new WebSocket(address, protocols);
                    const send = socket.send.bind(socket);
                    socket.send = (payload) => {
                        if (!announced) {
                            const hello = serializer.deserialize(payload).marshal();
                            check(hello[0] === 1, "first outgoing packet was not HELLO");
                            announced = hello[2].roles;
                        }
                        send(payload);
                    };
                    return socket;
                },
            }).connect(uri, "realm");
            try {
                const expected = {
                    caller: [
                        "call_canceling",
                        "caller_identification",
                        "progressive_call_invocations",
                        "progressive_call_results",
                    ],
                    callee: [
                        "call_canceling",
                        "caller_identification",
                        "pattern_based_registration",
                        "shared_registration",
                        "progressive_call_invocations",
                        "progressive_call_results",
                    ],
                    publisher: ["publisher_exclusion", "publisher_identification", "subscriber_blackwhite_listing"],
                    subscriber: ["pattern_based_subscription", "publisher_identification"],
                };
                check(
                    Object.keys(announced).sort().join() === Object.keys(expected).sort().join(),
                    "unexpected HELLO roles",
                );
                for (const [role, features] of Object.entries(expected)) {
                    const actual = announced[role].features;
                    check(Object.keys(actual).sort().join() === features.sort().join(), `incorrect ${role} features`);
                    check(
                        Object.values(actual).every((value) => value === true),
                        `invalid ${role} feature value`,
                    );
                }
                check(
                    typeof session.details.sessionID === "number" && session.details.sessionID > 0,
                    "invalid session ID",
                );
                check(session.details.realm === "realm", "realm changed");
                const result = await session.call("echo").args("public API").execute();
                check(result instanceof Result && result.args[0] === "public API", "authenticated RPC failed");
                authenticatedRpcRoundTrips++;
            } finally {
                await session.close();
            }
        }
        const client = new Client({ serializer, connectTimeout: 5000, closeTimeout: 500 });
        const caller = await client.connect(uri, "realm");
        const callee = await client.connect(uri, "realm");
        caller.onError((error) => errors.push(error));
        callee.onError((error) => errors.push(error));
        const procedure = `browser.echo.${variant}.${serializer.constructor.name}`;
        const topic = `${procedure}.events`;
        try {
            let fluentCalls = 0;
            const pendingRegistration = callee
                .register(`${procedure}.fluent`, (inv) => {
                    fluentCalls++;
                    return new Result(inv.args, inv.kwargs);
                })
                .invoke("single");
            const fluentRegistration = await pendingRegistration.execute();
            const request = caller.call(`${procedure}.fluent`).args("fluent").kwarg("enabled", true);
            check(fluentCalls === 0, "builder sent a call before execute");
            const fluentResult = await request.execute();
            check(
                fluentCalls === 1 && fluentResult.args[0] === "fluent" && fluentResult.kwargs.enabled === true,
                "fluent call lost payload",
            );
            const fluentEvent = deferred();
            const fluentSubscription = await callee
                .subscribe(`${topic}.fluent`, fluentEvent.resolve)
                .match("exact")
                .execute();
            const publication = caller.publish(`${topic}.fluent`).args("event").acknowledge();
            await publication.execute();
            check((await fluentEvent.promise).args[0] === "event", "fluent publication lost event");
            await fluentSubscription.unsubscribe();
            await fluentRegistration.unregister();
            await rejects(
                () => caller.call(`${procedure}.missing`).execute(),
                (error) => error instanceof ApplicationError,
                "fluent remote error",
            );
            // Reentrant handlers must not block reception of their nested call result.
            const registration = await callee
                .register(procedure, async (inv) => {
                    await callee.call("echo").args("nested").execute();
                    return new Result(inv.args, inv.kwargs);
                })
                .execute();
            const results = await Promise.all(
                Array.from({ length: 12 }, (_, index) =>
                    caller.call(procedure).args(index).kwargs({ enabled: true }).execute(),
                ),
            );
            check(
                results.every((result, index) => result.args[0] === index && result.kwargs.enabled),
                "concurrent calls changed",
            );
            await rejects(
                () => callee.register(procedure, () => new Result()).execute(),
                (error) => error instanceof ApplicationError,
                "duplicate registration",
            );
            const first = deferred();
            const second = deferred();
            let firstCount = 0;
            const a = await callee
                .subscribe(topic, (event) => {
                    firstCount++;
                    first.resolve(event);
                })
                .execute();
            const b = await callee.subscribe(topic, (event) => second.resolve(event)).execute();
            check(a.subscriptionID === b.subscriptionID, "router should share subscription IDs");
            await caller.publish(topic).args("event").kwargs({}).options({ acknowledge: true }).execute();
            check(
                (await first.promise).args[0] === "event" && (await second.promise).args[0] === "event",
                "event lost",
            );
            await a.unsubscribe();
            const remaining = deferred();
            const c = await callee.subscribe(topic, (event) => remaining.resolve(event)).execute();
            check(
                (await caller.publish(topic).args("remaining").execute()) === null,
                "unacknowledged publish return changed",
            );
            check(
                (await remaining.promise).args[0] === "remaining" && firstCount === 1,
                "subscription removal affected siblings",
            );
            await b.unsubscribe();
            await c.unsubscribe();
            const errorRegistration = await callee
                .register(`${procedure}.error`, () => {
                    throw new ApplicationError("com.example.failure", { args: ["reason"], kwargs: { code: 7 } });
                })
                .execute();
            await rejects(
                () => caller.call(`${procedure}.error`).execute(),
                (error) =>
                    error instanceof ApplicationError &&
                    error.message === "com.example.failure" &&
                    error.args[0] === "reason" &&
                    error.kwargs.code === 7,
                "callee error",
            );
            await errorRegistration.unregister();
            const progressRegistration = await callee
                .register(`${procedure}.progress`, async (inv) => {
                    await inv.sendProgress([1]);
                    await inv.sendProgress([2]);
                    return Result.create("done");
                })
                .execute();
            const updates = [];
            const result = await caller.callProgress(`${procedure}.progress`, async (value) => {
                await caller.call("echo").args("progress callback").execute();
                updates.push(value.args[0]);
            });
            check(result.args[0] === "done" && updates.join() === "1,2", "progressive results lost or reordered");
            const sentinel = new Error("progress callback failed");
            await rejects(
                () =>
                    caller.callProgress(`${procedure}.progress`, () => {
                        throw sentinel;
                    }),
                (error) => error === sentinel,
                "progress callback error",
            );
            await progressRegistration.unregister();
            const entered = deferred();
            const cancelled = deferred();
            const blocked = await callee
                .register(`${procedure}.wait`, async (inv) => {
                    entered.resolve();
                    await new Promise((resolve) => {
                        if (inv.signal.aborted) resolve();
                        else inv.signal.addEventListener("abort", resolve, { once: true });
                    });
                    cancelled.resolve();
                    return new Result();
                })
                .execute();
            const controller = new AbortController();
            const pending = caller.call(`${procedure}.wait`).signal(controller.signal).execute();
            const cancelledCall = rejects(
                () => pending,
                (error) => error.name === "AbortError",
                "caller cancellation",
            );
            await entered.promise;
            controller.abort();
            await cancelledCall;
            await cancelled.promise;
            await blocked.unregister();
            await registration.unregister();
            await rejects(
                () => caller.call(procedure).execute(),
                (error) => error instanceof ApplicationError && error.message === "wamp.error.no_such_procedure",
                "unregistered call",
            );
            check(
                (await caller.call("echo").args("healthy").execute()).args[0] === "healthy",
                "session became unusable",
            );
            let disconnected = 0;
            caller.onDisconnect(async () => {
                disconnected++;
            });
            await caller.close();
            await caller.close();
            check(disconnected === 1 && !caller.isConnected(), "disconnect lifecycle changed");
            sessions += 2;
        } finally {
            await caller.close();
            await callee.close();
        }
    }
    check(errors.length === 0, `unexpected callback errors: ${errors.map(String).join(", ")}`);
    return { variant, authenticatedRpcRoundTrips, lifecycleSessions: sessions };
}
