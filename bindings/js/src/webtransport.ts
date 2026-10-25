import type { ClientOptions } from "./client.js";
import { ProtocolError } from "./exception.js";
import { getNative, rustCall } from "./native.js";
import { connectPeer, payloadBytes } from "./peer.js";
import type { MessagePeer } from "./peer.js";
import { getSubProtocol } from "./serializers.js";
import type { Payload, Serializer } from "./serializers.js";
import type { Session } from "./session.js";
import { abortable, timeoutValue } from "./util.js";
import type { RawSocketFraming } from "./wasm/xconn_browser.js";

export interface WebTransportOptions {
    serverCertificateHashes?: WebTransportHash[];
    connectTimeout?: number;
    signal?: AbortSignal;
}

/** A native browser byte stream. EOF is undefined, not an empty data packet. */
export class ByteStream {
    private readonly reader: ReadableStreamDefaultReader<Uint8Array>;
    private readonly writer: WritableStreamDefaultWriter<Uint8Array>;
    private closing?: Promise<void>;
    constructor(stream: WebTransportBidirectionalStream) {
        this.reader = stream.readable.getReader();
        this.writer = stream.writable.getWriter();
    }

    async read(): Promise<Uint8Array | undefined> {
        if (this.closing) {
            throw new ProtocolError("Byte stream closed");
        }

        const value = await this.reader.read();
        return value.done ? undefined : value.value;
    }

    async write(data: Uint8Array): Promise<void> {
        if (this.closing) {
            throw new ProtocolError("Byte stream closed");
        }

        await this.writer.write(data);
    }

    async writeEof(): Promise<void> {
        if (this.closing) {
            throw new ProtocolError("Byte stream closed");
        }

        await this.writer.close();
    }

    close(): Promise<void> {
        return (this.closing ??= (async () => {
            await Promise.allSettled([this.reader.cancel(), this.writer.abort()]);
            this.reader.releaseLock();
            this.writer.releaseLock();
        })());
    }
}

class ByteReader {
    private buffer: Uint8Array = new Uint8Array(0);
    constructor(private readonly stream: ByteStream) {}

    clear(): void {
        this.buffer = new Uint8Array(0);
    }

    async exact(size: number): Promise<Uint8Array> {
        const output = new Uint8Array(size);
        let offset = 0;
        while (offset < size) {
            if (this.buffer.length === 0) {
                const chunk = await this.stream.read();
                if (chunk === undefined) {
                    throw new ProtocolError("Transport stream ended");
                }

                this.buffer = chunk;
            }

            const count = Math.min(size - offset, this.buffer.length);
            output.set(this.buffer.subarray(0, count), offset);
            this.buffer = this.buffer.subarray(count);
            offset += count;
        }

        return output;
    }
}

class RawSocketPeer implements MessagePeer {
    private readonly input: ByteReader;
    private readonly framing: RawSocketFraming;
    private closed = false;
    private readonly listeners = new Set<(reason?: string) => void>();
    private writes = Promise.resolve();
    constructor(
        private readonly stream: ByteStream,
        serializer: Serializer,
    ) {
        this.input = new ByteReader(stream);
        this.framing = new (getNative().RawSocketFraming)(getSubProtocol(serializer).slice("wamp.2.".length));
    }

    async negotiate(signal: AbortSignal): Promise<void> {
        await abortable(this.stream.write(rustCall(() => this.framing.handshake())), signal);
        const response = await abortable(this.input.exact(4), signal);
        rustCall(() => this.framing.accept_handshake(response));
    }

    send(data: Payload): Promise<void> {
        return this.frame(0, payloadBytes(data));
    }

    private frame(kind: number, data: Uint8Array): Promise<void> {
        if (this.closed) {
            return Promise.reject(new ProtocolError("Transport closed"));
        }

        const header = rustCall(() => this.framing.header(kind, data.length));
        const frame = new Uint8Array(header.length + data.length);
        frame.set(header);
        frame.set(data, header.length);
        const result = this.writes.then(() => this.stream.write(frame));
        this.writes = result.catch(() => this.close());
        return result;
    }

    async receive(): Promise<Uint8Array> {
        try {
            while (!this.closed) {
                const header = await this.input.exact(4);
                if (this.closed) {
                    throw new ProtocolError("Transport closed");
                }

                const [kind, length] = rustCall(() => this.framing.parse_header(header));
                const data = await this.input.exact(length);
                if (kind === 0) {
                    return data;
                }

                if (kind === 1) {
                    await this.frame(2, data);
                }
            }

            throw new ProtocolError("Transport closed");
        } catch (error) {
            await this.close();
            throw error;
        }
    }

    onDisconnect(callback: (reason?: string) => void): void {
        if (this.closed) {
            callback("Transport closed");
        } else {
            this.listeners.add(callback);
        }
    }

    isConnected(): boolean {
        return !this.closed;
    }

    async close(): Promise<void> {
        if (this.closed) {
            return;
        }

        this.closed = true;
        this.input.clear();
        this.framing.free();
        for (const callback of this.listeners) {
            callback("Transport closed");
        }

        this.listeners.clear();
        await this.stream.close();
    }
}

/** Browser QUIC connectivity uses HTTP/3 WebTransport, not arbitrary QUIC sockets. */
export class WebTransportConnection {
    private readonly incoming: ReadableStreamDefaultReader<WebTransportBidirectionalStream>;
    private closed = false;
    private constructor(private readonly transport: WebTransport) {
        this.incoming = transport.incomingBidirectionalStreams.getReader();
        void transport.closed
            .catch(() => undefined)
            .finally(() => {
                this.closed = true;
            });
    }

    static async connect(url: string, options: WebTransportOptions = {}): Promise<WebTransportConnection> {
        if (typeof WebTransport === "undefined") {
            throw new ProtocolError("WebTransport is unavailable in this browser");
        }

        const controller = new AbortController();
        const abort = () => controller.abort(options.signal?.reason);
        options.signal?.addEventListener("abort", abort, { once: true });
        if (options.signal?.aborted) {
            abort();
        }

        const timeout = timeoutValue(options.connectTimeout ?? 20000, "connectTimeout");
        const timer =
            timeout > 0
                ? setTimeout(() => controller.abort(new ProtocolError("WebTransport connection timed out")), timeout)
                : undefined;
        let transport: WebTransport | undefined;
        try {
            controller.signal.throwIfAborted();
            transport = new WebTransport(url, { serverCertificateHashes: options.serverCertificateHashes });
            await abortable(transport.ready, controller.signal);
            return new WebTransportConnection(transport);
        } catch (error) {
            transport?.close();
            // Suppress a ready/closed rejection even when abort won the race.
            void transport?.closed.catch(() => undefined);
            throw error;
        } finally {
            clearTimeout(timer);
            options.signal?.removeEventListener("abort", abort);
        }
    }

    openSession(realm: string, options: ClientOptions = {}): Promise<Session> {
        return connectPeer(
            async (serializer, signal) => {
                const stream = await abortable(
                    this.openStream().then(async (value) => {
                        if (signal.aborted) {
                            await value.close();
                            signal.throwIfAborted();
                        }

                        return value;
                    }),
                    signal,
                );
                const peer = new RawSocketPeer(stream, serializer);
                try {
                    await peer.negotiate(signal);
                    return peer;
                } catch (error) {
                    await peer.close();
                    throw error;
                }
            },
            realm,
            options,
        );
    }

    async openStream(): Promise<ByteStream> {
        if (this.closed) {
            throw new ProtocolError("WebTransport connection closed");
        }

        return new ByteStream(await this.transport.createBidirectionalStream());
    }

    async acceptStream(): Promise<ByteStream> {
        const result = await this.incoming.read();
        if (result.done) {
            throw new ProtocolError("WebTransport connection closed");
        }

        return new ByteStream(result.value);
    }

    async close(): Promise<void> {
        if (this.closed) {
            return;
        }

        this.closed = true;
        this.transport.close();
        await this.incoming.cancel().catch(() => undefined);
        await this.transport.closed.catch(() => undefined);
    }
}
