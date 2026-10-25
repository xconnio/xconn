import type { ClientOptions } from "./client.js";
import { ProtocolError } from "./exception.js";
import { Inbox } from "./inbox.js";
import { getNative, initializeWasm, rustCall } from "./native.js";
import { connectPeer, payloadBytes } from "./peer.js";
import type { MessagePeer } from "./peer.js";
import { getSubProtocol } from "./serializers.js";
import type { Payload, Serializer } from "./serializers.js";
import { Session } from "./session.js";
import type { SessionOptions } from "./session.js";
import type { BaseSession, Subscription } from "./types.js";
import { abortable, timeoutValue } from "./util.js";
import type { ChannelFraming, RawSocketFraming } from "./wasm/xconn_browser.js";

function byteLength(data: string | Uint8Array): number {
    return typeof data === "string" ? new TextEncoder().encode(data).length : data.length;
}

function asError(error: unknown): Error {
    return error instanceof Error ? error : new ProtocolError(String(error));
}

/** Browser-owned RTCDataChannel with Rust-owned chunk framing. */
export class DataChannel {
    readonly opened: Promise<void>;
    private readonly inbox = new Inbox<string | Uint8Array>(byteLength, 32 * 1024 * 1024, 256);
    private readonly listeners = new Set<(reason?: string) => void>();
    private decoder?: ChannelFraming;
    private encoder?: ChannelFraming;
    private writes = Promise.resolve();
    private closing?: Promise<void>;
    private closed = false;
    private reading = false;
    private queuedBytes = 0;
    private queuedWrites = 0;
    private readonly bufferWaiters = new Map<symbol, number>();
    private resolveOpen!: () => void;
    private rejectOpen!: (error: Error) => void;

    constructor(
        private readonly channel: RTCDataChannel,
        private readonly receiveLimit?: number,
    ) {
        this.opened = new Promise((resolve, reject) => {
            this.resolveOpen = resolve;
            this.rejectOpen = reject;
        });
        void this.opened.catch(() => undefined);
        channel.binaryType = "arraybuffer";
        channel.addEventListener("open", this.onOpen);
        channel.addEventListener("message", this.onMessage);
        channel.addEventListener("error", this.onError);
        channel.addEventListener("close", this.onClose);
        if (channel.readyState === "open") {
            this.onOpen();
        } else if (channel.readyState === "closed") {
            this.onClose();
        }
    }

    get label(): string {
        return this.channel.label;
    }

    get protocol(): string {
        return this.channel.protocol;
    }

    isConnected(): boolean {
        return !this.closed && this.channel.readyState === "open";
    }

    private readonly onOpen = () => this.resolveOpen();

    private readonly onClose = () => this.stop(new ProtocolError("Data channel closed"));

    private readonly onError = () => {
        this.stop(new ProtocolError("Data channel failed"));
        this.channel.close();
    };

    private readonly onMessage = (event: MessageEvent) => {
        try {
            const value =
                typeof event.data === "string"
                    ? event.data
                    : event.data instanceof ArrayBuffer
                      ? new Uint8Array(event.data)
                      : undefined;
            if (value === undefined) {
                throw new ProtocolError("Unsupported data-channel packet");
            }

            if (typeof value === "string" && value.length > getNative().channel_packet_limit()) {
                throw new ProtocolError("Data-channel packet exceeds its limit");
            }

            const size = byteLength(value);
            if (size > getNative().channel_packet_limit()) {
                throw new ProtocolError("Data-channel packet exceeds its limit");
            }

            this.inbox.push(value);
        } catch (error) {
            this.stop(asError(error));
            this.channel.close();
        }
    };

    private stop(error: Error): void {
        if (this.closed) {
            return;
        }

        this.closed = true;
        this.rejectOpen(error);
        this.inbox.close(error);
        this.decoder?.free();
        this.encoder?.free();
        this.decoder = undefined;
        this.encoder = undefined;
        this.channel.removeEventListener("open", this.onOpen);
        this.channel.removeEventListener("message", this.onMessage);
        this.channel.removeEventListener("close", this.onClose);
        this.channel.removeEventListener("error", this.onError);
        for (const callback of this.listeners) {
            callback(error.message);
        }

        this.listeners.clear();
    }

    onDisconnect(callback: (reason?: string) => void): void {
        if (this.closed) {
            callback("Data channel closed");
        } else {
            this.listeners.add(callback);
        }
    }

    /** Bytes waiting in both the owned write queue and the browser transport. */
    get bufferedAmount(): number {
        return this.queuedBytes + this.channel.bufferedAmount;
    }

    /** Wait for preceding writes and browser backpressure to drain. */
    async waitBufferedBelow(threshold: number): Promise<void> {
        if (!Number.isFinite(threshold) || threshold < 0) {
            throw new RangeError("Buffer threshold must be a nonnegative finite number");
        }

        await this.writes;
        if (!this.isConnected()) {
            throw new ProtocolError("Data channel closed");
        }

        await this.bufferedBelow(threshold);
    }

    private bufferedBelow(threshold: number): Promise<void> {
        if (this.channel.bufferedAmount <= threshold) {
            return Promise.resolve();
        }

        return new Promise((resolve, reject) => {
            const token = Symbol();
            const refreshThreshold = () => {
                this.channel.bufferedAmountLowThreshold = this.bufferWaiters.size
                    ? Math.min(...this.bufferWaiters.values())
                    : 256 * 1024;
            };

            this.bufferWaiters.set(token, threshold);
            refreshThreshold();
            const cleanup = () => {
                this.channel.removeEventListener("bufferedamountlow", low);
                this.channel.removeEventListener("close", closed);
                this.bufferWaiters.delete(token);
                refreshThreshold();
            };

            const low = () => {
                if (this.channel.bufferedAmount <= threshold) {
                    cleanup();
                    resolve();
                }
            };

            const closed = () => {
                cleanup();
                reject(new ProtocolError("Data channel closed"));
            };

            this.channel.addEventListener("bufferedamountlow", low);
            this.channel.addEventListener("close", closed);
            if (this.channel.readyState === "closed") {
                closed();
            } else {
                low();
            }
        });
    }

    private async packet(data: string | Uint8Array): Promise<void> {
        const size = byteLength(data);
        if (size > getNative().channel_packet_limit()) {
            throw new ProtocolError("Data-channel packet exceeds its limit");
        }

        await this.bufferedBelow(256 * 1024);
        if (!this.isConnected()) {
            throw new ProtocolError("Data channel closed");
        }

        if (typeof data === "string") {
            this.channel.send(data);
        } else {
            this.channel.send(new Uint8Array(data));
        }
    }

    private enqueue(data: string | Uint8Array, write: (owned: string | Uint8Array) => Promise<void>): Promise<void> {
        if (this.closed || this.closing) {
            return Promise.reject(new ProtocolError("Data channel closed"));
        }

        const size = byteLength(data);
        if (this.queuedWrites >= 256 || this.queuedBytes + size > 32 * 1024 * 1024) {
            return Promise.reject(new ProtocolError("Data-channel write queue exceeded its limit"));
        }

        const owned = typeof data === "string" ? data : data.slice();
        this.queuedBytes += size;
        this.queuedWrites++;
        const result = this.writes
            .then(() => {
                if (this.closed) {
                    throw new ProtocolError("Data channel closed");
                }

                return write(owned);
            })
            .finally(() => {
                this.queuedBytes -= size;
                this.queuedWrites--;
            });
        this.writes = result.catch((error) => {
            this.stop(asError(error));
            this.channel.close();
        });
        return result;
    }

    send(data: string | Uint8Array): Promise<void> {
        return this.enqueue(data, (owned) => this.packet(owned));
    }

    private reliable(): void {
        if (!this.channel.ordered || this.channel.maxRetransmits !== null || this.channel.maxPacketLifeTime !== null) {
            throw new ProtocolError("Chunked messages require an ordered, reliable channel");
        }
    }

    writeMessage(data: Uint8Array): Promise<void> {
        this.reliable();
        return this.enqueue(data, async (owned) => {
            this.encoder ??= new (getNative().ChannelFraming)();
            const chunks = rustCall(() => this.encoder!.encode(owned as Uint8Array)) as Uint8Array[];
            for (const chunk of chunks) {
                await this.packet(chunk);
            }
        });
    }

    async receive(signal?: AbortSignal): Promise<string | Uint8Array> {
        if (this.closed) {
            throw new ProtocolError("Data channel closed");
        }

        if (this.reading) {
            throw new ProtocolError("A channel read is already pending");
        }

        this.reading = true;
        try {
            return await this.inbox.receive(signal);
        } finally {
            this.reading = false;
        }
    }

    async readMessage(signal?: AbortSignal): Promise<Uint8Array> {
        if (this.closed) {
            throw new ProtocolError("Data channel closed");
        }

        this.reliable();
        if (this.reading) {
            throw new ProtocolError("A channel read is already pending");
        }

        this.reading = true;
        try {
            this.decoder ??= new (getNative().ChannelFraming)(this.receiveLimit);
            for (;;) {
                const packet = await this.inbox.receive(signal);
                if (this.closed) {
                    throw new ProtocolError("Data channel closed");
                }

                if (typeof packet === "string") {
                    throw new ProtocolError("Text packet in a chunked message");
                }

                const result = rustCall(() => this.decoder!.feed(packet));
                if (result !== undefined) {
                    return result;
                }
            }
        } catch (error) {
            if (!signal?.aborted) {
                this.stop(asError(error));
                this.channel.close();
            }

            throw error;
        } finally {
            this.reading = false;
        }
    }

    close(): Promise<void> {
        return (this.closing ??= (async () => {
            let timer: ReturnType<typeof setTimeout> | undefined;
            try {
                await Promise.race([
                    this.writes.then(() => this.bufferedBelow(0)),
                    new Promise<void>((resolve) => {
                        timer = setTimeout(resolve, 2000);
                    }),
                ]);
            } finally {
                clearTimeout(timer);
                this.stop(new ProtocolError("Data channel closed"));
                this.channel.close();
            }
        })());
    }
}

class ChannelPeer implements MessagePeer {
    private readonly framing: RawSocketFraming;

    constructor(
        private readonly channel: DataChannel,
        serializer: Serializer,
    ) {
        this.framing = new (getNative().RawSocketFraming)(getSubProtocol(serializer).slice("wamp.2.".length));
        channel.onDisconnect(() => this.framing.free());
    }

    async negotiate(signal: AbortSignal): Promise<void> {
        await abortable(this.channel.opened, signal);
        await abortable(this.channel.send(rustCall(() => this.framing.handshake())), signal);
        const response = await this.channel.receive(signal);
        if (typeof response === "string") {
            throw new ProtocolError("Text RawSocket handshake");
        }

        rustCall(() => this.framing.accept_handshake(response));
    }

    send(payload: Payload): Promise<void> {
        const data = payloadBytes(payload);
        if (data.length > this.framing.send_limit()) {
            throw new ProtocolError("WAMP payload exceeds peer limit");
        }

        return this.channel.writeMessage(data);
    }

    receive(): Promise<Uint8Array> {
        return this.channel.readMessage();
    }

    onDisconnect(callback: (reason?: string) => void): void {
        this.channel.onDisconnect(callback);
    }

    isConnected(): boolean {
        return this.channel.isConnected();
    }

    close(): Promise<void> {
        return this.channel.close();
    }
}

export interface WebRTCOptions extends ClientOptions {
    offerProcedure: string;
    remoteCandidatesTopic: string;
    localCandidatesTopic: string;
    iceServers?: RTCIceServer[];
    openTimeout?: number;
    handshakeTimeout?: number;
}

export interface ChannelOptions extends RTCDataChannelInit {
    openTimeout?: number;
    signal?: AbortSignal;
}

export class WebRTCSession extends Session {
    constructor(
        base: BaseSession,
        readonly connection: WebRTCConnection,
        options: SessionOptions = {},
    ) {
        super(base, options);
    }
}

/** Browser ICE, DTLS and SCTP; WAMP and message framing remain in Rust/WASM. */
export class WebRTCConnection {
    private readonly incoming = new Inbox<DataChannel>(() => 1, 256, 256);
    private readonly channels = new Set<DataChannel>();
    private initial?: DataChannel;
    private subscription?: Subscription;
    private closing?: Promise<void>;
    private closed = false;
    private setup?: AbortController;
    private requestID?: string;
    private publishingActive = false;
    private remoteQueued = 0;
    private remote = Promise.resolve();
    private publishing = Promise.resolve();
    private readonly remotePending: Array<[string, RTCIceCandidateInit]> = [];
    private readonly localPending: RTCIceCandidateInit[] = [];

    private constructor(
        private readonly peer: RTCPeerConnection,
        private readonly signaling: Session,
        private readonly options: WebRTCOptions,
    ) {
        peer.addEventListener("datachannel", (event) => {
            if (event.channel.protocol !== getNative().raw_channel_protocol()) {
                event.channel.close();
                return;
            }

            const channel = this.track(event.channel);
            try {
                this.incoming.push(channel);
            } catch {
                void channel.close();
                void this.close();
            }
        });
        peer.addEventListener("connectionstatechange", () => {
            if (peer.connectionState === "failed" || peer.connectionState === "closed") {
                void this.close();
            }
        });
        peer.addEventListener("icecandidate", (event) => {
            if (this.closed) {
                return;
            }

            const candidate = event.candidate?.toJSON() ?? { candidate: "" };
            if (!this.publishingActive) {
                if (this.localPending.length >= 256) {
                    void this.close();
                    return;
                }

                this.localPending.push(candidate);
            } else {
                this.publish(candidate);
            }
        });
    }

    private track(channel: RTCDataChannel, wamp = false): DataChannel {
        const wrapped = new DataChannel(channel, wamp ? getNative().rawsocket_receive_limit() : undefined);
        this.channels.add(wrapped);
        wrapped.onDisconnect(() => this.channels.delete(wrapped));
        return wrapped;
    }

    private publish(candidate: RTCIceCandidateInit): void {
        this.publishing = this.publishing
            .then(async () => {
                if (this.closed) {
                    return;
                }

                const json = rustCall(() => getNative().signaling_encode(candidate));
                await this.signaling.publish(this.options.localCandidatesTopic).args([this.requestID, json]).execute();
            })
            .catch(() => {
                void this.close();
            });
    }

    private candidate(id: string, json: string): void {
        if (this.closed) {
            return;
        }

        if (++this.remoteQueued > 256) {
            void this.close();
            return;
        }

        // A single chain preserves ordering across early-candidate activation and live trickle.
        this.remote = this.remote
            .then(async () => {
                if (this.closed) {
                    return;
                }

                const candidate = rustCall(() => getNative().signaling_decode(json)) as RTCIceCandidateInit;
                if (typeof candidate.candidate !== "string") {
                    throw new ProtocolError("Invalid ICE candidate");
                }

                if (!this.requestID) {
                    if (this.remotePending.length >= 256) {
                        throw new ProtocolError("Too many pending ICE candidates");
                    }

                    this.remotePending.push([id, candidate]);
                } else if (id === this.requestID) {
                    await this.peer.addIceCandidate(candidate.candidate ? candidate : null);
                }
            })
            .catch(() => {
                void this.close();
            })
            .finally(() => {
                this.remoteQueued--;
            });
    }

    static async connect(signaling: Session, realm: string, options: WebRTCOptions): Promise<WebRTCSession> {
        if (typeof RTCPeerConnection === "undefined") {
            throw new ProtocolError("WebRTC is unavailable in this runtime");
        }

        for (const value of [options.offerProcedure, options.remoteCandidatesTopic, options.localCandidatesTopic]) {
            if (typeof value !== "string" || !value) {
                throw new ProtocolError("WebRTC offer and candidate endpoints are required");
            }
        }

        const timeout = timeoutValue(options.connectTimeout ?? 20000, "connectTimeout");
        timeoutValue(options.openTimeout ?? 20000, "openTimeout");
        timeoutValue(options.handshakeTimeout ?? 5000, "handshakeTimeout");
        const controller = new AbortController();
        const abort = () => controller.abort(options.signal?.reason);
        options.signal?.addEventListener("abort", abort, { once: true });
        if (options.signal?.aborted) {
            abort();
        }

        const timer =
            timeout > 0
                ? setTimeout(() => controller.abort(new ProtocolError("WebRTC connection timed out")), timeout)
                : undefined;
        let connection: WebRTCConnection | undefined;
        try {
            controller.signal.throwIfAborted();
            await abortable(initializeWasm(), controller.signal);
            connection = new WebRTCConnection(
                new RTCPeerConnection({ iceServers: options.iceServers }),
                signaling,
                options,
            );
            const current = connection;
            current.setup = controller;
            current.initial = current.track(current.peer.createDataChannel("data"), true);
            const pending = signaling
                .subscribe(options.remoteCandidatesTopic, (event) => {
                    const [id, json] = event.args ?? [];
                    if (typeof id === "string" && typeof json === "string") {
                        current.candidate(id, json);
                    }
                })
                .execute()
                .then((subscription) => {
                    if (current.closed) {
                        void subscription.unsubscribe().catch(() => undefined);
                    } else {
                        current.subscription = subscription;
                    }

                    return subscription;
                });
            await abortable(pending, controller.signal);
            const offer = await abortable(current.peer.createOffer(), controller.signal);
            await abortable(current.peer.setLocalDescription(offer), controller.signal);
            const payload = rustCall(() => getNative().signaling_encode({ description: offer, candidates: [] }));
            const result = await signaling
                .call(options.offerProcedure)
                .arg(payload)
                .signal(controller.signal)
                .execute();
            if (typeof result.args?.[0] !== "string") {
                throw new ProtocolError("Invalid WebRTC offer response");
            }

            const response = rustCall(() => getNative().signaling_decode(result.args![0] as string));
            if (
                typeof response.requestID !== "string" ||
                !response.requestID ||
                response.answer?.description?.type !== "answer"
            ) {
                throw new ProtocolError("Invalid WebRTC answer");
            }

            await abortable(current.peer.setRemoteDescription(response.answer.description), controller.signal);
            current.remote = current.remote.then(async () => {
                current.requestID = response.requestID;
                for (const candidate of response.answer.candidates ?? []) {
                    await current.peer.addIceCandidate(candidate.candidate ? candidate : null);
                }

                for (const [id, candidate] of current.remotePending.splice(0)) {
                    if (id === current.requestID) {
                        await current.peer.addIceCandidate(candidate.candidate ? candidate : null);
                    }
                }
            });
            await abortable(current.remote, controller.signal);
            for (const candidate of current.localPending.splice(0)) {
                current.publish(candidate);
            }

            current.publishingActive = true;
            const session = await current.openSession(realm, { ...options, signal: controller.signal });
            return session;
        } catch (error) {
            await connection?.close();
            throw error;
        } finally {
            if (connection) {
                connection.setup = undefined;
            }

            clearTimeout(timer);
            options.signal?.removeEventListener("abort", abort);
        }
    }

    openSession(realm: string, options: ClientOptions = {}): Promise<WebRTCSession> {
        return connectPeer(
            async (serializer, signal) => {
                if (this.closed) {
                    throw new ProtocolError("WebRTC connection closed");
                }

                const channel = this.initial ?? this.track(this.peer.createDataChannel("data"), true);
                this.initial = undefined;
                const peer = new ChannelPeer(channel, serializer);
                try {
                    await this.waitChannel(channel, this.options.openTimeout, signal);
                    const controller = new AbortController();
                    const abort = () => controller.abort(signal.reason);
                    signal.addEventListener("abort", abort, { once: true });
                    if (signal.aborted) {
                        abort();
                    }

                    const deadline = timeoutValue(this.options.handshakeTimeout ?? 5000, "handshakeTimeout");
                    const timer = setTimeout(
                        () => controller.abort(new ProtocolError("WebRTC handshake timed out")),
                        deadline,
                    );
                    try {
                        await peer.negotiate(controller.signal);
                    } finally {
                        clearTimeout(timer);
                        signal.removeEventListener("abort", abort);
                    }

                    return peer;
                } catch (error) {
                    await peer.close();
                    throw error;
                }
            },
            realm,
            { connectTimeout: this.options.openTimeout ?? 20000, ...options },
            (base, config) => new WebRTCSession(base, this, config),
        );
    }

    async openChannel(label: string, options: ChannelOptions = {}): Promise<DataChannel> {
        if (this.closed) {
            throw new ProtocolError("WebRTC connection closed");
        }

        const { signal, openTimeout, ...config } = options;
        const channel = this.track(
            this.peer.createDataChannel(label, { ...config, protocol: getNative().raw_channel_protocol() }),
        );
        try {
            await this.waitChannel(channel, openTimeout, signal);
            return channel;
        } catch (error) {
            await channel.close();
            throw error;
        }
    }

    async acceptChannel(options: { openTimeout?: number; signal?: AbortSignal } = {}): Promise<DataChannel> {
        const channel = await this.incoming.receive(options.signal);
        try {
            await this.waitChannel(channel, options.openTimeout, options.signal);
            return channel;
        } catch (error) {
            await channel.close();
            throw error;
        }
    }

    private async waitChannel(channel: DataChannel, timeout?: number, signal?: AbortSignal): Promise<void> {
        const milliseconds = timeoutValue(timeout ?? this.options.openTimeout ?? 20000, "openTimeout");
        const controller = new AbortController();
        const abort = () => controller.abort(signal?.reason);
        signal?.addEventListener("abort", abort, { once: true });
        if (signal?.aborted) {
            abort();
        }

        const timer =
            milliseconds > 0
                ? setTimeout(() => controller.abort(new ProtocolError("Data channel open timed out")), milliseconds)
                : undefined;
        try {
            await abortable(channel.opened, controller.signal);
        } finally {
            clearTimeout(timer);
            signal?.removeEventListener("abort", abort);
        }
    }

    close(): Promise<void> {
        return (this.closing ??= (async () => {
            this.closed = true;
            this.setup?.abort(new ProtocolError("WebRTC connection closed"));
            this.peer.onicecandidate = null;
            this.peer.close();
            this.incoming.close(new ProtocolError("WebRTC connection closed"));
            await Promise.allSettled([...this.channels].map((channel) => channel.close()));
            await this.subscription?.unsubscribe().catch(() => undefined);
            this.remotePending.length = 0;
            this.localPending.length = 0;
        })());
    }
}
