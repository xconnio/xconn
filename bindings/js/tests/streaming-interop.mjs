function check(condition, message) {
    if (!condition) throw new Error(message);
}

async function failure(operation, fragment) {
    try {
        await operation();
    } catch (error) {
        check(String(error).includes(fragment), `Expected ${fragment}, got ${error}`);
        return error;
    }
    throw new Error(`Expected ${fragment}`);
}

export async function exerciseStreamingInterop(api, uri, variant) {
    if (!uri) return { skipped: "ci/test-browser.sh builds the Rust peer" };
    const serializers =
        variant === "json"
            ? [new api.JSONSerializer()]
            : [new api.JSONSerializer(), new api.CBORSerializer(), new api.MsgPackSerializer()];
    for (const serializer of serializers) {
        const session = await new api.Client({ serializer }).connect(uri, "realm");
        try {
            const nested = { nested: [true, null, "مرحبا 🦀"] };
            const call = await session
                .call("rust.peer.duplex")
                .args("first", nested)
                .kwarg("marker", "initial")
                .stream({ progressiveInput: true });
            try {
                const first = await call.receive();
                check(
                    first.progress && JSON.stringify(first.args) === JSON.stringify(["first", nested]),
                    "Rust changed initial payload",
                );
                check(first.kwargs.marker === "initial", "Rust lost initial kwargs");
                const receiving = call.receive();
                await call.send(["second"], { marker: "middle" });
                const middle = await receiving;
                check(
                    middle.progress && middle.args[0] === "second" && middle.kwargs.marker === "middle",
                    "duplex path changed payload",
                );
                await call.finish(["last"], { marker: "final" });
                const frames = [];
                for await (const frame of call.results()) frames.push(frame);
                check(frames.length === 2 && frames[0].progress && !frames[1].progress, "wrong final-result sequence");
                check(frames[0].args[0] === "last" && frames[0].kwargs.marker === "final", "last input was lost");
                check(frames[1].args[0] === "done" && frames[1].kwargs.complete, "final payload was lost");
                check((await call.receive()) === null, "final result repeated");
            } finally {
                await call.close();
            }
            const failed = await session.call("rust.peer.fail").stream();
            try {
                const before = await failed.receive();
                check(before.progress && before.args[0] === "before-error", "progress before error was lost");
                const error = await failure(() => failed.receive(), "example.failure");
                check(
                    error instanceof api.ApplicationError && error.args[0] === "detail" && error.kwargs.enabled,
                    "Rust error payload changed",
                );
                check((await failed.receive()) === null, "application error repeated");
            } finally {
                await failed.close();
            }
            for (const kind of ["cancel", "deadline"]) {
                const token = `${kind}-${crypto.randomUUID()}`;
                const waiting = await session
                    .call("rust.peer.wait")
                    .arg(token)
                    .timeout(kind === "deadline" ? 100 : 5000)
                    .stream();
                try {
                    check((await waiting.receive()).args[0] === "started", "Rust handler did not start");
                    if (kind === "cancel") await waiting.close();
                    await failure(() => waiting.receive(), kind === "cancel" ? "cancelled" : "timed out");
                    const deadline = Date.now() + 3000;
                    while (!(await session.call("rust.peer.cancelled").arg(token).execute()).args[0]) {
                        check(Date.now() < deadline, "Rust handler did not observe cancellation");
                        await new Promise((resolve) => setTimeout(resolve, 10));
                    }
                } finally {
                    await waiting.close();
                }
            }
            const cancelled = new Set();
            await session
                .register("js.peer.duplex", async (inv) => {
                    await inv.sendProgress(inv.args, inv.kwargs);
                    return inv.details.progress ? undefined : new api.Result(["done"], { complete: true });
                })
                .execute();
            await session
                .register("js.peer.fail", async (inv) => {
                    await inv.sendProgress(["before-error"]);
                    throw new api.ApplicationError("example.failure", { args: ["detail"], kwargs: { enabled: true } });
                })
                .execute();
            await session
                .register("js.peer.wait", async (inv) => {
                    const token = inv.args[0];
                    try {
                        await inv.sendProgress(["started"]);
                        await new Promise((resolve) => {
                            if (inv.signal.aborted) resolve();
                            else inv.signal.addEventListener("abort", resolve, { once: true });
                        });
                    } finally {
                        cancelled.add(token);
                    }
                })
                .execute();
            await session
                .register("js.peer.cancelled", (inv) => api.Result.create(cancelled.has(inv.args[0])))
                .execute();
            const verified = await session.call("rust.peer.verify_js").timeout(15000).execute();
            check(verified.args[0] === "verified", "Rust caller did not verify browser handlers");
            check(
                cancelled.has("cancel") && cancelled.has("deadline"),
                "browser handlers did not observe Rust cancellation",
            );
        } finally {
            await session.close();
        }
    }
    return { directions: ["JS to Rust", "Rust to JS"], codecs: serializers.length };
}
