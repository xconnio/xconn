import { AnonymousAuthenticator } from "./auth.js";
import type { ClientAuthenticator } from "./auth.js";
import type { ClientOptions } from "./client.js";
import { Code } from "./constants.js";
import { ProtocolError } from "./exception.js";
import { WireMessage, inspectMessage } from "./message.js";
import type { Message } from "./message.js";
import { getNative, initializeWasm, rustCall } from "./native.js";
import { defaultSerializer, getSubProtocol, JSONSerializer } from "./serializers.js";
import type { Payload, Serializer } from "./serializers.js";
import { Session } from "./session.js";
import type { SessionOptions } from "./session.js";
import { BaseSession, SessionDetails } from "./types.js";
import { abortable, timeoutValue } from "./util.js";
import type { ClientProtocol } from "./wasm/xconn_browser.js";

/** Complete WAMP messages; transport-specific framing stays below this interface. */
export interface MessagePeer {
    send(data: Payload): void | Promise<void>;
    receive(): Promise<Payload>;
    close(): Promise<void>;
    onDisconnect(callback: (reason?: string) => void): void;
    isConnected(): boolean;
}

export function payloadBytes(payload: Payload): Uint8Array {
    return typeof payload === "string" ? new TextEncoder().encode(payload) : payload;
}

// Byte-oriented transports carry JSON as UTF-8; the JSON serializer expects text.
function codecPayload(codec: Serializer, data: Payload): Payload {
    return codec instanceof JSONSerializer && data instanceof Uint8Array ? new TextDecoder().decode(data) : data;
}

class PeerSession extends BaseSession {
    constructor(
        private readonly peer: MessagePeer,
        private readonly details: SessionDetails,
        private readonly codec: Serializer,
    ) {
        super();
    }

    getSessionDetails(): SessionDetails {
        return this.details;
    }

    serializer(): Serializer {
        return this.codec;
    }

    isConnected(): boolean {
        return this.peer.isConnected();
    }

    send(payload: Payload): void {
        void Promise.resolve(this.peer.send(payload)).catch(() => this.peer.close().catch(() => undefined));
    }

    async receive(): Promise<Payload> {
        return codecPayload(this.codec, await this.peer.receive());
    }

    sendMessage(message: Message): void {
        this.send(this.codec.serialize(message));
    }

    onDisconnect(callback: (reason?: string) => void): void {
        this.peer.onDisconnect(callback);
    }

    close(): Promise<void> {
        return this.peer.close();
    }
}

/** Common Rust-driven join path for WebSocket, WebTransport and WebRTC. */
export async function connectPeer<T extends Session = Session>(
    factory: (serializer: Serializer, signal: AbortSignal) => Promise<MessagePeer>,
    realm: string,
    options: ClientOptions = {},
    createSession?: (base: BaseSession, options: SessionOptions) => T,
): Promise<T> {
    const timeout = timeoutValue(options.connectTimeout ?? 20000, "connectTimeout");
    const closeTimeout = timeoutValue(options.closeTimeout ?? 10000, "closeTimeout");
    const controller = new AbortController();
    const externalAbort = () => controller.abort(options.signal?.reason);
    options.signal?.addEventListener("abort", externalAbort, { once: true });
    if (options.signal?.aborted) {
        externalAbort();
    }

    const timer =
        timeout > 0
            ? setTimeout(() => controller.abort(new ProtocolError("Connection timed out")), timeout)
            : undefined;
    let peer: MessagePeer | undefined;
    let protocol: ClientProtocol | undefined;
    let ownedSerializer: Serializer | undefined;
    let ownedAuthenticator: ClientAuthenticator | undefined;
    let transferred = false;
    let joining = true;
    try {
        controller.signal.throwIfAborted();
        await abortable(initializeWasm(), controller.signal);
        const serializer = options.serializer ?? (ownedSerializer = defaultSerializer());
        const auth = options.authenticator ?? (ownedAuthenticator = new AnonymousAuthenticator());
        protocol = rustCall(
            () =>
                new (getNative().ClientProtocol)(
                    realm,
                    getSubProtocol(serializer).slice("wamp.2.".length),
                    auth.authMethod,
                    auth.authID,
                    auth.authExtra,
                ),
        );
        const joiner = protocol;
        // Factories must release transport resources if setup is aborted.
        peer = await factory(serializer, controller.signal);
        peer.onDisconnect((reason) => {
            if (joining) {
                controller.abort(new ProtocolError(reason ?? "Connection closed"));
            }
        });
        controller.signal.throwIfAborted();
        await abortable(
            Promise.resolve(peer.send(serializer.serialize(new WireMessage(rustCall(() => joiner.hello()))))),
            controller.signal,
        );
        while (!joiner.joined()) {
            const payload = codecPayload(serializer, await abortable(peer.receive(), controller.signal));
            const message = inspectMessage(serializer.deserialize(payload));
            if (message.type() === Code.CHALLENGE) {
                rustCall(() => joiner.check_challenge());
                if (message.authmethod !== auth.authMethod) {
                    throw new ProtocolError("Unexpected authentication method");
                }

                const response = await abortable(auth.authenticate(message), controller.signal);
                rustCall(() => joiner.set_authentication(response.marshal()));
            }

            const response = rustCall(() => joiner.receive(message.marshal()));
            if (response !== undefined) {
                await abortable(
                    Promise.resolve(peer.send(serializer.serialize(new WireMessage(response)))),
                    controller.signal,
                );
            }
        }

        const details = rustCall(() => joiner.details());
        const base = new PeerSession(
            peer,
            new SessionDetails(
                details.sessionID,
                details.realm,
                details.authid,
                details.authrole,
                details.authmethod,
                details.authextra,
                details.roles,
            ),
            serializer,
        );
        const session = createSession
            ? createSession(base, { closeTimeout })
            : (new Session(base, { closeTimeout }) as T);
        const codec = ownedSerializer;
        session.onDisconnect(() => codec?.free?.());
        transferred = true;
        return session;
    } finally {
        joining = false;
        clearTimeout(timer);
        options.signal?.removeEventListener("abort", externalAbort);
        protocol?.free();
        ownedAuthenticator?.free?.();
        if (!transferred) {
            ownedSerializer?.free?.();
            await peer?.close().catch(() => undefined);
        }
    }
}
