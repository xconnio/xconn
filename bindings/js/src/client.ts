import { AnonymousAuthenticator, TicketAuthenticator, WAMPCRAAuthenticator, CryptoSignAuthenticator } from "./auth.js";
import type { ClientAuthenticator } from "./auth.js";
import { getSubProtocol } from "./serializers.js";
import type { Serializer } from "./serializers.js";
import { ProtocolError } from "./exception.js";
import { SocketInbox, sendPayload } from "./transport.js";
import type { Session } from "./session.js";
import { connectPeer } from "./peer.js";
import { abortable } from "./util.js";

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
    return connectPeer(
        async (serializer, signal) => {
            const subprotocol = getSubProtocol(serializer);
            const socket = options.webSocketFactory?.(uri, [subprotocol]) ?? new WebSocket(uri, [subprotocol]);
            const inbox = new SocketInbox(socket);
            try {
                await abortable(inbox.opened, signal);
                if (socket.protocol !== subprotocol) {
                    throw new ProtocolError("Router did not negotiate the requested WAMP subprotocol");
                }

                return {
                    send: (data) => sendPayload(socket, data),
                    receive: () => inbox.receive(),
                    onDisconnect: (callback) => inbox.onDisconnect(callback),
                    isConnected: () => inbox.connected(),
                    close: async () => {
                        inbox.stop(new ProtocolError("Connection closed"));
                        if (socket.readyState < WebSocket.CLOSING) {
                            socket.close();
                        }
                    },
                };
            } catch (error) {
                inbox.stop(new ProtocolError("Connection setup failed"));
                if (socket.readyState < WebSocket.CLOSING) {
                    socket.close();
                }

                throw error;
            }
        },
        realm,
        options,
    );
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
