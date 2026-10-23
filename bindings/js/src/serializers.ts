import { getNative, rustCall } from "./native.js";
import { ProtocolError } from "./exception.js";
import { type Message, WireMessage } from "./message.js";
import type { NativeCodec } from "./wasm/xconn_browser.js";

export type Payload = string | Uint8Array;

export interface Serializer {
    /** Release native storage. Built-in adapters recreate it lazily if reused. */
    free?(): void;

    serialize(message: Message): Payload;

    deserialize(payload: Payload): Message;
}

abstract class RustSerializer implements Serializer {
    private codec?: NativeCodec;
    abstract readonly codecName: string;

    get subprotocol(): string {
        return `wamp.2.${this.codecName}`;
    }

    private get native(): NativeCodec {
        return (this.codec ??= rustCall(() => new (getNative().NativeCodec)(this.codecName)));
    }

    free(): void {
        this.codec?.free();
        this.codec = undefined;
    }

    serialize(message: Message): Payload {
        const bytes = rustCall(() => this.native.encode(message.marshal()));
        return this.codecName === "json" ? new TextDecoder().decode(bytes) : bytes;
    }

    deserialize(payload: Payload): WireMessage {
        const bytes = typeof payload === "string" ? new TextEncoder().encode(payload) : payload;
        return new WireMessage(rustCall(() => this.native.decode(bytes)));
    }
}

export class JSONSerializer extends RustSerializer {
    readonly codecName = "json";
}

export class CBORSerializer extends RustSerializer {
    readonly codecName = "cbor";
}

export class MsgPackSerializer extends RustSerializer {
    readonly codecName = "msgpack";
}

export function getSubProtocol(serializer: Serializer): string {
    if (
        serializer instanceof JSONSerializer ||
        serializer instanceof CBORSerializer ||
        serializer instanceof MsgPackSerializer
    ) {
        return serializer.subprotocol;
    }

    throw new ProtocolError("invalid serializer");
}

export function defaultSerializer(): Serializer {
    return getNative().has_binary_codecs() ? new CBORSerializer() : new JSONSerializer();
}
