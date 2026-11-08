import { ProtocolError } from "./exception.js";

/** Reliable byte I/O, independent of the connection carrying it. */
export interface RPCStream {
    read(): Promise<Uint8Array | undefined>;
    write(data: Uint8Array): Promise<void>;
    writeEof(): Promise<void>;
    close(): Promise<void>;
}

export type StreamOpener = () => Promise<RPCStream>;

const key = "io.xconn.stream.v1";

function token(reference: unknown): Uint8Array {
    if (typeof reference !== "object" || reference === null || Object.keys(reference).length !== 1) {
        throw new ProtocolError("Argument is not a stream");
    }
    const encoded = (reference as Record<string, unknown>)[key];
    if (typeof encoded !== "string" || !/^[a-fA-F0-9]{64}$/.test(encoded)) {
        throw new ProtocolError("Invalid stream reference");
    }
    return Uint8Array.from(encoded.match(/../g)!, (byte) => parseInt(byte, 16));
}

async function connect(open: StreamOpener, bytes: Uint8Array): Promise<RPCStream> {
    const stream = await open();
    try {
        await stream.write(new Uint8Array([0x7f, 0, 0, 0, ...bytes]));
        return stream;
    } catch (error) {
        await stream.close();
        throw error;
    }
}

export async function offerStream(open: StreamOpener): Promise<{ reference: unknown; stream: RPCStream }> {
    const bytes = crypto.getRandomValues(new Uint8Array(32));
    const reference = { [key]: Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("") };
    return { reference, stream: await connect(open, bytes) };
}

export function acceptStream(open: StreamOpener, reference: unknown): Promise<RPCStream> {
    return connect(open, token(reference));
}
