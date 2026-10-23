// Compile the fluent public API and structural custom adapter types.
import {
    Client,
    Session,
    BaseSession,
    SessionDetails,
    Result,
    Registration,
    Subscription,
    Invocation,
    Event,
    JSONSerializer,
    CBORSerializer,
    MsgPackSerializer,
    AnonymousAuthenticator,
    TicketAuthenticator,
    WAMPCRAAuthenticator,
    CryptoSignAuthenticator,
    ApplicationError,
    ProtocolError,
    WebSocketBaseSession,
    connectAnonymous,
    connectTicket,
    connectCRA,
    connectCryptosign,
} from "../src/index.js";
import type { Call, StreamOptions, ClientAuthenticator, Serializer, Message } from "../src/index.js";

export async function publicApi(uri: string, base: BaseSession): Promise<void> {
    const serializers: Serializer[] = [new JSONSerializer(), new CBORSerializer(), new MsgPackSerializer()];
    const authenticators: ClientAuthenticator[] = [
        new AnonymousAuthenticator("", {}),
        new TicketAuthenticator("user", "ticket", null),
        new WAMPCRAAuthenticator("user", "secret", null),
        new CryptoSignAuthenticator("user", "11".repeat(32), {}),
    ];
    const client = new Client({ serializer: serializers[0], authenticator: authenticators[0] });
    const session: Session = await client.connect(uri, "realm");
    const reg: Registration = await session
        .register("procedure", async (inv: Invocation): Promise<Result> => new Result(inv.args, inv.kwargs))
        .execute();
    const sub: Subscription = await session
        .subscribe("topic", (event: Event) => {
            console.log(event.args);
        })
        .execute();
    const result: Result = await session
        .call("procedure")
        .args(1)
        .kwargs({ enabled: true })
        .options({ timeout: 1000 })
        .execute();
    await session
        .publish("topic")
        .args(...(result.args ?? []))
        .kwargs(result.kwargs)
        .options({ acknowledge: true })
        .execute();
    const streamOptions: StreamOptions = { progressiveInput: true, bufferSize: 16 };
    const call: Call = await session.call("procedure").arg("first").stream(streamOptions);
    try {
        await call.send(["second"], { enabled: true });
        await call.finish();
        for await (const frame of call.results()) console.log(frame.progress, frame.args);
        const finished: Result | null = await call.receive();
        console.log(finished);
    } finally {
        await call.close();
    }
    await reg.unregister();
    await sub.unsubscribe();
    await session.unregister(reg);
    await session.unsubscribe(sub);
    session.onDisconnect(async (reason) => {
        console.log(reason);
    });
    session.isConnected();
    void session.details.sessionID;
    await session.close();
    await session.call("procedure").args(1, 2).kwargs({ enabled: true }).timeout(1000).execute();
    await session
        .register("procedure", (inv) => new Result(inv.args))
        .invoke("single")
        .execute();
    await session
        .subscribe("topic", (event) => console.log(event.args))
        .match("exact")
        .execute();
    await session.publish("topic").args("hello").acknowledge().execute();
    new Session(base);
    new WebSocketBaseSession(
        new WebSocket(uri),
        () => undefined,
        new SessionDetails(1, "realm", "", "anonymous"),
        serializers[0],
    );
    new ApplicationError("application.failure", { args: ["reason"], kwargs: { code: 1 } });
    new ProtocolError("invalid packet");
    await connectAnonymous(uri, "realm");
    await connectTicket(uri, "realm", "user", "ticket");
    await connectCRA(uri, "realm", "user", "secret");
    await connectCryptosign(uri, "realm", "user", "11".repeat(32));
}

// Old message/auth classes are accepted structurally; no second protocol package is required.
export class CustomSerializer implements Serializer {
    constructor(private readonly delegate: Serializer) {}

    serialize(message: Message) {
        return this.delegate.serialize(message);
    }

    deserialize(payload: string | Uint8Array) {
        return this.delegate.deserialize(payload);
    }
}
