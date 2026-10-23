export async function exerciseDiscovery(module, uri) {
    const session = await module.connectAnonymous(uri, "example");
    try {
        const discovery = session.discovery("example.peers");
        const endpoint = {
            id: "browser-sensor",
            addresses: ["quic://sensor:443"],
            capabilities: ["telemetry"],
            metadata: { version: 1 },
        };
        await discovery.advertise(endpoint);
        const found = await discovery.get(endpoint.id);
        if (
            found.id !== endpoint.id ||
            found.metadata.version !== 1 ||
            found.addresses[0] !== endpoint.addresses[0] ||
            found.capabilities[0] !== "telemetry"
        ) {
            throw new Error("Discovery endpoint changed across the WAMP service boundary");
        }
        if (!(await discovery.list()).some((value) => value.id === endpoint.id)) {
            throw new Error("Discovery omitted an advertised endpoint");
        }
        await discovery.withdraw(endpoint.id);
        if ((await discovery.list()).some((value) => value.id === endpoint.id)) {
            throw new Error("Discovery retained a withdrawn endpoint");
        }
        return "passed";
    } finally {
        await session.close();
    }
}
