function check(condition, message) {
    if (!condition) throw new Error(message);
}

async function rejects(operation, fragment) {
    try {
        await operation();
    } catch (error) {
        check(String(error).includes(fragment), `Expected ${fragment}, got ${error}`);
        return error;
    }
    throw new Error(`Expected rejection: ${fragment}`);
}

export async function exerciseStreaming(api, uri) {
    if (!uri) return { skipped: true };
    const session = await new api.Client().connect(uri, "realm");
    try {
        await session
            .register("js.stream.duplex", async (inv) => {
                await inv.sendProgress(inv.args);
                return inv.details.progress ? undefined : api.Result.create("done");
            })
            .execute();
        const call = await session.call("js.stream.duplex").arg("first").stream({ progressiveInput: true });
        const first = await call.receive();
        check(first.progress && first.args[0] === "first", "initial input was lost");
        const receiving = call.receive();
        await rejects(() => call.receive(), "pending");
        await call.send(["second"]);
        check((await receiving).args[0] === "second", "duplex send blocked on receive");
        await call.finish(["last"]);
        const frames = [];
        for await (const frame of call.results()) frames.push(frame);
        check(
            frames.length === 2 && frames[0].progress && !frames[1].progress,
            "final result not delivered exactly once",
        );
        check(frames[1].args[0] === "done" && (await call.receive()) === null, "completion was lost");
        await rejects(() => call.send([]), "finished");

        for (const progressiveInput of [false, true]) {
            for (const progressiveResults of [false, true]) {
                const procedure = `js.stream.modes.${progressiveInput}.${progressiveResults}`;
                await session
                    .register(procedure, async (invocation) => {
                        check(
                            Boolean(invocation.details.receive_progress) === progressiveResults,
                            "wrong receive_progress on wire",
                        );
                        if (progressiveResults) await invocation.sendProgress(["partial"]);
                        return invocation.details.progress ? undefined : api.Result.create("final");
                    })
                    .execute();
                const stream = await session.call(procedure).option("receive_progress", !progressiveResults).stream({
                    progressiveInput,
                    progressiveResults,
                });
                if (progressiveInput) {
                    if (progressiveResults) check((await stream.receive()).progress, "first progress missing");
                    await stream.finish();
                }
                if (progressiveResults) check((await stream.receive()).progress, "progress missing");
                const result = await stream.receive();
                check(!result.progress && result.args[0] === "final", "final-only reply missing");
                check((await stream.receive()) === null, "completion missing");
            }
        }

        await session
            .register("js.stream.fail", () => {
                throw new api.ApplicationError("example.failure", { args: ["detail"], kwargs: { enabled: true } });
            })
            .execute();
        const failed = await session.call("js.stream.fail").stream();
        const error = await rejects(() => failed.receive(), "example.failure");
        check(error instanceof api.ApplicationError && error.args[0] === "detail", "application error payload lost");
        check((await failed.receive()) === null, "application error delivered twice");

        await session
            .register("js.stream.wait", async (inv) => {
                await inv.sendProgress(["started"]);
                await new Promise((resolve) => {
                    if (inv.signal.aborted) resolve();
                    else inv.signal.addEventListener("abort", resolve, { once: true });
                });
            })
            .execute();
        const waiting = await session.call("js.stream.wait").stream();
        for await (const frame of waiting) {
            check(frame.args[0] === "started", "unexpected progress");
            break;
        }
        await rejects(() => waiting.receive(), "cancelled");
        check((await waiting.receive()) === null, "cancellation delivered twice");
        const timed = await session.call("js.stream.wait").timeout(30).stream();
        await timed.receive();
        await rejects(() => timed.receive(), "timed out");

        let floodSent;
        const sent = new Promise((resolve) => {
            floodSent = resolve;
        });
        await session
            .register("js.stream.flood", async (inv) => {
                for (let n = 0; n < 8; n++) await inv.sendProgress([n]);
                floodSent();
                return api.Result.create("done");
            })
            .execute();
        const flood = await session.call("js.stream.flood").stream({ bufferSize: 1 });
        await sent;
        // An ordinary call is a barrier after the flood handler sends its frames.
        await session.call("echo").arg("healthy").execute();
        await flood.receive();
        await rejects(() => flood.receive(), "overflow");
        check(
            (await session.call("echo").arg("healthy").execute()).args[0] === "healthy",
            "stream failure broke session",
        );
        await rejects(() => session.call("echo").stream({ bufferSize: 0 }), "buffer");
        await rejects(
            () =>
                session
                    .call("echo")
                    .onProgress(() => {})
                    .stream(),
            "callbacks",
        );
        await session.close();
        await rejects(() => session.call("echo").stream(), "closed");
        return { duplex: true, errors: true, cancellation: true, overflow: true };
    } finally {
        if (session.isConnected()) await session.close();
    }
}
