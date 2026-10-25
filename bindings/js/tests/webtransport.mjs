function check(value, message) {
    if (!value) throw new Error(message);
}

export async function exerciseWebTransport(api, peer, variant) {
    const cert = Uint8Array.from(peer.webtransport_certificate.match(/../g), (v) => parseInt(v, 16));
    const hash = await globalThis.crypto.subtle.digest("SHA-256", cert);
    const options = { serverCertificateHashes: [{ algorithm: "sha-256", value: hash }], connectTimeout: 5000 };
    const aborted = new AbortController();
    aborted.abort();
    let rejected = false;
    try {
        await api.WebTransportConnection.connect(peer.webtransport, { ...options, signal: aborted.signal });
    } catch {
        rejected = true;
    }
    check(rejected, "pre-aborted WebTransport attempt succeeded");
    let untrusted = false;
    try {
        await api.WebTransportConnection.connect(peer.webtransport, {
            connectTimeout: 1000,
            serverCertificateHashes: [{ algorithm: "sha-256", value: new Uint8Array(32) }],
        });
    } catch {
        untrusted = true;
    }
    check(untrusted, "WebTransport accepted an incorrect certificate hash");
    const connection = await api.WebTransportConnection.connect(peer.webtransport, options);
    const sessions = [];
    try {
        const serializers =
            variant === "json"
                ? [new api.JSONSerializer()]
                : [new api.JSONSerializer(), new api.CBORSerializer(), new api.MsgPackSerializer()];
        for (const serializer of serializers) {
            const session = await connection.openSession("realm", { serializer });
            sessions.push(session);
            const large = "x".repeat(200000);
            check(
                (await session.call("echo").arg(large).execute()).args[0] === large,
                "fragmented RawSocket payload mismatch",
            );
            const values = await Promise.all([1, 2, 3].map((v) => session.call("echo").arg(v).execute()));
            check(
                values.every((v, i) => v.args[0] === i + 1),
                "WebTransport concurrent RPC correlation failed",
            );
        }
        const subscriber = sessions[0];
        const callee = await connection.openSession("realm");
        sessions.push(callee);
        const registration = await callee
            .register("browser.wt.echo", (inv) => new api.Result(inv.args, inv.kwargs))
            .execute();
        check(
            (await subscriber.call("browser.wt.echo").arg("callee").execute()).args[0] === "callee",
            "WebTransport callee failed",
        );
        await registration.unregister();
        const progressive = await callee
            .register("browser.wt.progress", async (inv) => {
                await inv.sendProgress(["partial"]);
                return new api.Result(["final"]);
            })
            .execute();
        const call = await subscriber.call("browser.wt.progress").stream();
        const results = [];
        for await (const result of call.results()) results.push(result);
        check(
            results.length === 2 && results[0].progress && !results[1].progress,
            "WebTransport progressive results failed",
        );
        await progressive.unregister();
        await subscriber.close();
        check(
            (await callee.call("echo").arg("sibling").execute()).args[0] === "sibling",
            "closing a session closed its connection",
        );
        const stream = await connection.openStream();
        try {
            await stream.write(new TextEncoder().encode("browser bytes"));
            await stream.writeEof();
            const received = [];
            for (;;) {
                const chunk = await stream.read();
                if (chunk === undefined) break;
                received.push(...chunk);
            }
            check(
                new TextDecoder().decode(new Uint8Array(received)) === "browser bytes",
                "WebTransport raw stream mismatch",
            );
        } finally {
            await stream.close();
        }
        return { codecs: serializers.length, concurrentRPC: true, callee: true, rawStreams: true, abort: true };
    } finally {
        await Promise.allSettled(sessions.map((s) => s.close()));
        await connection.close();
    }
}
