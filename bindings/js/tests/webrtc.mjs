function check(value, message) {
    if (!value) throw new Error(message);
}

function same(a, b) {
    return a.length === b.length && a.every((value, index) => value === b[index]);
}

async function exerciseConcurrentBackpressure(api) {
    class Channel extends EventTarget {
        label = "backpressure";
        protocol = "";
        readyState = "open";
        bufferedAmount = 1000;
        bufferedAmountLowThreshold = 0;
        close() {
            this.readyState = "closed";
            this.dispatchEvent(new Event("close"));
        }
    }
    const native = new Channel();
    const channel = new api.DataChannel(native);
    try {
        const drained = channel.waitBufferedBelow(0);
        const writable = channel.waitBufferedBelow(100);
        await Promise.resolve();
        check(native.bufferedAmountLowThreshold === 0, "concurrent buffer waits overwrote the lower threshold");
        native.bufferedAmount = 0;
        native.dispatchEvent(new Event("bufferedamountlow"));
        await Promise.all([drained, writable]);
    } finally {
        await channel.close();
    }
}

export async function exerciseWebRTC(api, server, variant) {
    await exerciseConcurrentBackpressure(api);
    const signaling = await api.connect(server.ws, "realm");
    const observer = await api.connect(server.ws, "realm");
    const options = {
        offerProcedure: "offer",
        remoteCandidatesTopic: "to-client",
        localCandidatesTopic: "to-provider",
        connectTimeout: 10000,
        openTimeout: 5000,
    };
    const localCandidates = [];
    const monitor = await observer
        .subscribe("to-provider", (event) => {
            localCandidates.push(event.args);
        })
        .execute();
    let peer;
    const sessions = [];
    const channels = [];
    try {
        const aborted = new AbortController();
        aborted.abort();
        let rejected = false;
        try {
            await api.WebRTCConnection.connect(signaling, "realm", { ...options, signal: aborted.signal });
        } catch {
            rejected = true;
        }
        check(rejected, "pre-aborted WebRTC setup succeeded");
        let failed = false;
        try {
            await api.WebRTCConnection.connect(signaling, "realm", { ...options, offerProcedure: "missing.offer" });
        } catch {
            failed = true;
        }
        check(failed && signaling.isConnected(), "failed WebRTC setup damaged borrowed signaling");
        peer = await api.WebRTCConnection.connect(signaling, "realm", {
            ...options,
            serializer: new api.JSONSerializer(),
        });
        sessions.push(peer);
        check(
            (await peer.call("echo").arg("browser RTC").execute()).args[0] === "browser RTC",
            "initial RTC WAMP call failed",
        );
        const serializers =
            variant === "json"
                ? [new api.JSONSerializer()]
                : [new api.JSONSerializer(), new api.CBORSerializer(), new api.MsgPackSerializer()];
        for (const serializer of serializers) {
            const session = await peer.connection.openSession("realm", { serializer });
            sessions.push(session);
            const large = "rtc".repeat(50000);
            check((await session.call("echo").arg(large).execute()).args[0] === large, "chunked WAMP RPC failed");
        }
        const largeResult = await peer.call("large_result").execute();
        check(largeResult.args[0] === "rtc".repeat(700000), "multi-megabyte WebRTC response changed");
        check(
            (await peer.call("echo").arg("after large response").execute()).args[0] === "after large response",
            "large WebRTC response disconnected the session",
        );
        check(localCandidates.length > 0, "browser did not trickle ICE candidates through WAMP");
        for (const args of localCandidates) {
            check(
                args.length === 2 && typeof args[0] === "string" && typeof args[1] === "string",
                "ICE events must contain separate request ID and candidate JSON arguments",
            );
            check(typeof JSON.parse(args[1]).candidate === "string", "ICE event contains invalid candidate JSON");
        }
        const second = sessions[1];
        const registration = await second
            .register("browser.rtc.echo", (inv) => new api.Result(inv.args, inv.kwargs))
            .execute();
        check((await peer.call("browser.rtc.echo").arg("callee").execute()).args[0] === "callee", "RTC callee failed");
        await registration.unregister();
        const progressive = await second
            .register("browser.rtc.progress", async (inv) => {
                await inv.sendProgress(["partial"]);
                return new api.Result(["final"]);
            })
            .execute();
        const call = await peer.call("browser.rtc.progress").stream();
        const results = [];
        for await (const result of call.results()) results.push(result);
        check(results.length === 2 && results[0].progress && !results[1].progress, "RTC progressive results failed");
        await progressive.unregister();
        const packets = await peer.connection.openChannel("packets");
        channels.push(packets);
        let invalidThreshold = false;
        try {
            await packets.waitBufferedBelow(-1);
        } catch (error) {
            invalidThreshold = error instanceof RangeError;
        }
        check(invalidThreshold, "negative buffer threshold was accepted");
        await packets.send("text packet");
        await packets.waitBufferedBelow(0);
        check(packets.bufferedAmount === 0, "RTC writes did not drain");
        check((await packets.receive()) === "text packet", "RTC text packet lost its type");
        await packets.send(new Uint8Array([0, 255]));
        check(same(await packets.receive(), new Uint8Array([0, 255])), "RTC binary packet mismatch");
        const data = await peer.connection.openChannel("files");
        channels.push(data);
        const first = Uint8Array.from({ length: 400000 }, (_, i) => i % 256);
        const last = Uint8Array.from({ length: 200000 }, (_, i) => (i + 1) % 256);
        await Promise.all([data.writeMessage(first), data.writeMessage(last)]);
        check(same(await data.readMessage(), first), "RTC concurrent chunks interleaved");
        check(same(await data.readMessage(), last), "RTC second chunked message mismatch");
        await data.writeMessage(new Uint8Array());
        check((await data.readMessage()).length === 0, "empty RTC message lost");
        const cancel = new AbortController();
        const pending = data.readMessage(cancel.signal);
        cancel.abort();
        try {
            await pending;
            throw new Error("cancelled RTC receive succeeded");
        } catch (error) {
            check(cancel.signal.reason === error, "wrong RTC cancellation error");
        }
        await data.writeMessage(new Uint8Array([9]));
        check((await data.readMessage())[0] === 9, "cancelled RTC read consumed later data");
        const trigger = await peer.connection.openChannel("trigger-remote");
        channels.push(trigger);
        const incoming = await peer.connection.acceptChannel();
        channels.push(incoming);
        check(
            new TextDecoder().decode(await incoming.readMessage()) === "server first",
            "server-initiated channel lost its first message",
        );
        await peer.close();
        check(
            (await second.call("echo").arg("sibling").execute()).args[0] === "sibling",
            "closing RTC session closed siblings",
        );
        await data.writeMessage(new Uint8Array([42]));
        check((await data.readMessage())[0] === 42, "WAMP close closed raw channel");
        await peer.connection.close();
        check(!second.isConnected(), "connection close left WAMP session connected");
        check(
            (await signaling.call("echo").arg("signaling alive").execute()).args[0] === "signaling alive",
            "RTC close closed signaling session",
        );
        return {
            codecs: serializers.length,
            trickleICE: true,
            rawPackets: true,
            chunking: true,
            remoteChannels: true,
            cancellation: true,
            cleanup: true,
        };
    } finally {
        await Promise.allSettled(channels.map((channel) => channel.close()));
        await Promise.allSettled(sessions.map((session) => session.close()));
        await peer?.connection.close();
        await monitor.unsubscribe();
        await observer.close();
        await signaling.close();
    }
}
