import { ProtocolError } from "./exception.js";
import { BaseSession, SessionDetails } from "./types.js";
import type { Message } from "./message.js";
import type { Payload, Serializer } from "./serializers.js";

export function sendPayload(socket: WebSocket, payload: Payload): void {
    if (typeof payload === "string") {
        socket.send(payload);
        return;
    }

    const buffer = payload.buffer;
    const bytes =
        buffer instanceof ArrayBuffer
            ? new Uint8Array(buffer, payload.byteOffset, payload.byteLength)
            : Uint8Array.from(payload);
    socket.send(bytes);
}

interface Waiter {
    resolve: (value: Payload) => void;
    reject: (error: unknown) => void;
}

/** One ordered receiver stays installed across the join/session handoff. */
export class SocketInbox {
    private readonly queue: { value: Payload; size: number }[] = [];
    private readonly waiters: Waiter[] = [];
    private readonly listeners = new Set<(reason?: string) => void>();
    private queuedBytes = 0;
    private failure?: Error;
    private chain: Promise<void> = Promise.resolve();
    private resolveOpen!: () => void;
    private rejectOpen!: (error: unknown) => void;
    readonly opened = new Promise<void>((resolve, reject) => {
        this.resolveOpen = resolve;
        this.rejectOpen = reject;
    });

    constructor(private readonly socket: WebSocket) {
        socket.binaryType = "arraybuffer";
        socket.addEventListener("open", this.onOpen);
        socket.addEventListener("message", this.onMessage);
        socket.addEventListener("close", this.onClose);
        socket.addEventListener("error", this.onError);

        if (socket.readyState === WebSocket.OPEN) {
            this.resolveOpen();
        }
    }

    private readonly onOpen = () => this.resolveOpen();
    private readonly onMessage = (event: MessageEvent) => {
        this.chain = this.chain
            .then(async () => {
                let value: Payload;

                if (typeof event.data === "string" || event.data instanceof Uint8Array) {
                    value = event.data;
                } else if (event.data instanceof ArrayBuffer) {
                    value = new Uint8Array(event.data);
                } else if (event.data instanceof Blob) {
                    value = new Uint8Array(await event.data.arrayBuffer());
                } else {
                    throw new ProtocolError("Unsupported WebSocket payload");
                }

                if (this.failure) {
                    return;
                }

                const waiter = this.waiters.shift();

                if (waiter) {
                    waiter.resolve(value);
                } else {
                    const size = this.size(value);
                    this.queuedBytes += size;

                    if (this.queue.length >= 1024 || this.queuedBytes > 64 * 1024 * 1024) {
                        throw new ProtocolError("WebSocket receive queue exceeded its limit");
                    }

                    this.queue.push({ value, size });
                }
            })
            .catch((error) => {
                this.stop(error instanceof Error ? error : new ProtocolError(String(error)));
                this.socket.close();
            });
    };
    private readonly onClose = (event: CloseEvent) => {
        void this.chain.then(() => this.stop(new ProtocolError(event.reason || "Connection closed")));
    };
    private readonly onError = () => {
        void this.chain.then(() => this.stop(new ProtocolError("WebSocket connection failed")));
    };

    private size(value: Payload): number {
        return typeof value === "string" ? new TextEncoder().encode(value).length : value.byteLength;
    }

    receive(): Promise<Payload> {
        const queued = this.queue.shift();

        if (queued !== undefined) {
            this.queuedBytes -= queued.size;
            return Promise.resolve(queued.value);
        }

        if (this.failure) {
            return Promise.reject(this.failure);
        }

        return new Promise((resolve, reject) => this.waiters.push({ resolve, reject }));
    }

    onDisconnect(callback: (reason?: string) => void): void {
        if (this.failure) {
            queueMicrotask(() => callback(this.failure?.message));
        } else {
            this.listeners.add(callback);
        }
    }

    connected(): boolean {
        return !this.failure && this.socket.readyState === WebSocket.OPEN;
    }

    stop(error: Error): void {
        if (this.failure) {
            return;
        }

        this.failure = error;
        this.rejectOpen(error);
        this.socket.removeEventListener("open", this.onOpen);
        this.socket.removeEventListener("message", this.onMessage);
        this.socket.removeEventListener("close", this.onClose);
        this.socket.removeEventListener("error", this.onError);

        for (const waiter of this.waiters.splice(0)) {
            waiter.reject(error);
        }

        this.queue.length = 0;
        this.queuedBytes = 0;

        for (const callback of this.listeners) {
            callback(error.message);
        }

        this.listeners.clear();
    }
}

export class WebSocketBaseSession extends BaseSession {
    private readonly inbox: SocketInbox;

    constructor(
        private readonly socket: WebSocket,
        messageHandler: unknown,
        private readonly sessionDetails: SessionDetails,
        private readonly codec: Serializer,
    ) {
        super();

        if (messageHandler instanceof SocketInbox) {
            this.inbox = messageHandler;
        } else {
            if (typeof messageHandler === "function") {
                socket.removeEventListener("message", messageHandler as EventListener);
            }

            this.inbox = new SocketInbox(socket);
            // An externally supplied socket may already be closed before anyone awaits opened.
            void this.inbox.opened.catch(() => undefined);
        }
    }

    id(): number {
        return this.sessionDetails.sessionID;
    }

    realm(): string {
        return this.sessionDetails.realm;
    }

    authid(): string {
        return this.sessionDetails.authid;
    }

    authrole(): string {
        return this.sessionDetails.authrole;
    }

    serializer(): Serializer {
        return this.codec;
    }

    getSessionDetails(): SessionDetails {
        return this.sessionDetails;
    }

    isConnected(): boolean {
        return this.inbox.connected();
    }

    send(data: Payload): void {
        if (!this.isConnected()) {
            throw new ProtocolError("Connection closed");
        }

        sendPayload(this.socket, data);
    }

    receive(): Promise<Payload> {
        return this.inbox.receive();
    }

    sendMessage(message: Message): void {
        this.send(this.codec.serialize(message));
    }

    async receiveMessage(): Promise<Message> {
        return this.codec.deserialize(await this.receive());
    }

    onDisconnect(callback: (reason?: string) => void | Promise<void>): void {
        this.inbox.onDisconnect((reason) => {
            void Promise.resolve()
                .then(() => callback(reason))
                .catch((error) => console.error(error));
        });
    }

    async close(): Promise<void> {
        this.inbox.stop(new ProtocolError("Connection closed"));

        if (this.socket.readyState < WebSocket.CLOSING) {
            this.socket.close();
        }
    }
}
