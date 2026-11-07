function check(value, message) {
    if (!value) throw new Error(message);
}

export async function exerciseRPCStreams(session) {
    let invalid = false;
    try {
        await session.openStreamArgument({});
    } catch {
        invalid = true;
    }
    check(invalid, "invalid stream reference was accepted");
    const result = await session.call("rpc.stream").execute();
    const stream = await result.argStream(1);
    try {
        const data = Uint8Array.from({ length: 70000 }, (_, index) => index % 256);
        const receiving = (async () => {
            const bytes = [];
            for (;;) {
                const chunk = await stream.read();
                if (chunk === undefined) break;
                bytes.push(...chunk);
            }
            check(
                bytes.length === data.length && bytes.every((byte, index) => byte === data[index]),
                "RPC stream bytes changed",
            );
        })();
        await Promise.all([
            stream.write(data.subarray(0, 35000)),
            stream.write(data.subarray(35000)),
            stream.writeEof(),
        ]);
        await receiving;
    } finally {
        await stream.close();
    }
    const offered = await session.offerStream();
    const call = session.call("rpc.upload").arg(null).arg(offered.reference).execute();
    try {
        await offered.stream.write(new Uint8Array([0, 255, 42]));
        await offered.stream.writeEof();
        const reply = [];
        for (;;) {
            const chunk = await offered.stream.read();
            if (chunk === undefined) break;
            reply.push(...chunk);
        }
        check(reply.join() === "0,255,42", "RPC input stream bytes changed");
        await call;
    } finally {
        await offered.stream.close();
    }
}
