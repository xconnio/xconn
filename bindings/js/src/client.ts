import { AnonymousAuthenticator, TicketAuthenticator, WAMPCRAAuthenticator, CryptoSignAuthenticator } from "./auth.js";
import type { ClientAuthenticator } from "./auth.js";
import { defaultSerializer, getSubProtocol } from "./serializers.js";
import type { Serializer } from "./serializers.js";
import { getNative, initializeWasm, rustCall } from "./native.js";
import { WireMessage, inspectMessage } from "./message.js";
import { Code } from "./constants.js";
import { ProtocolError } from "./exception.js";
import { SessionDetails } from "./types.js";
import { WebSocketBaseSession, SocketInbox, sendPayload } from "./transport.js";
import { Session } from "./session.js";
import { abortable, timeoutValue } from "./util.js";
import type { ClientProtocol } from "./wasm/xconn_browser.js";

export interface ClientOptions {
    authenticator?: ClientAuthenticator;
    serializer?: Serializer;
    connectTimeout?: number;
    closeTimeout?: number;
    signal?: AbortSignal;
    webSocketFactory?: (uri: string, protocols: string[]) => WebSocket;
}

export class Client {
    constructor(private readonly options: ClientOptions = {}) {}

    connect(uri: string, realm: string): Promise<Session> {
        return connect(uri, realm, this.options);
    }
}

export async function connect(uri: string, realm: string, options: ClientOptions = {}): Promise<Session> {
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
    let socket: WebSocket | undefined;
    let inbox: SocketInbox | undefined;
    let protocol: ClientProtocol | undefined;
    let transferred = false;
    let ownedSerializer: Serializer | undefined;
    let ownedAuthenticator: ClientAuthenticator | undefined;

    try {
        controller.signal.throwIfAborted();
        await abortable(initializeWasm(), controller.signal);
        const serializer = options.serializer ?? (ownedSerializer = defaultSerializer());
        const authenticator = options.authenticator ?? (ownedAuthenticator = new AnonymousAuthenticator());
        const subprotocol = getSubProtocol(serializer);
        const joiner = (protocol = rustCall(
            () =>
                new (getNative().ClientProtocol)(
                    realm,
                    subprotocol.slice("wamp.2.".length),
                    authenticator.authMethod,
                    authenticator.authID,
                    authenticator.authExtra,
                ),
        ));
        socket = options.webSocketFactory?.(uri, [subprotocol]) ?? new WebSocket(uri, [subprotocol]);
        inbox = new SocketInbox(socket);
        await abortable(inbox.opened, controller.signal);

        if (socket.protocol !== subprotocol) {
            throw new ProtocolError("Router did not negotiate the requested WAMP subprotocol");
        }

        sendPayload(socket, serializer.serialize(new WireMessage(rustCall(() => joiner.hello()))));

        while (!joiner.joined()) {
            const payload = await abortable(inbox.receive(), controller.signal);
            const message = inspectMessage(serializer.deserialize(payload));

            if (message.type() === Code.CHALLENGE) {
                rustCall(() => joiner.check_challenge());

                if (message.authmethod !== authenticator.authMethod) {
                    throw new ProtocolError("Unexpected authentication method");
                }

                const response = await abortable(authenticator.authenticate(message), controller.signal);
                rustCall(() => joiner.set_authentication(response.marshal()));
            }

            const response = rustCall(() => joiner.receive(message.marshal()));

            if (response !== undefined) {
                sendPayload(socket, serializer.serialize(new WireMessage(response)));
            }
        }

        const details = rustCall(() => joiner.details());
        const base = new WebSocketBaseSession(
            socket,
            inbox,
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
        const session = new Session(base, { closeTimeout });
        const codec = ownedSerializer;
        session.onDisconnect(() => codec?.free?.());
        transferred = true;
        return session;
    } finally {
        clearTimeout(timer);
        options.signal?.removeEventListener("abort", externalAbort);
        protocol?.free();
        ownedAuthenticator?.free?.();

        if (!transferred) {
            ownedSerializer?.free?.();
            inbox?.stop(new ProtocolError("Connection setup failed"));

            if (socket && socket.readyState < WebSocket.CLOSING) {
                socket.close();
            }
        }
    }
}

export function connectAnonymous(uri: string, realm: string): Promise<Session> {
    return connect(uri, realm);
}

export function connectTicket(uri: string, realm: string, authid: string, ticket: string): Promise<Session> {
    const authenticator = new TicketAuthenticator(authid, ticket, null);
    return connect(uri, realm, { authenticator }).finally(() => authenticator.free());
}

export function connectCRA(uri: string, realm: string, authid: string, secret: string): Promise<Session> {
    const authenticator = new WAMPCRAAuthenticator(authid, secret, null);
    return connect(uri, realm, { authenticator }).finally(() => authenticator.free());
}

export function connectCryptosign(uri: string, realm: string, authid: string, key: string): Promise<Session> {
    const authenticator = new CryptoSignAuthenticator(authid, key, {});
    return connect(uri, realm, { authenticator }).finally(() => authenticator.free());
}
