import type { Dictionary } from "./message.js";

export class PayloadError extends Error {
    constructor(
        readonly kind: "missing" | "null" | "type" | "range",
        message: string,
    ) {
        super(message);
        this.name = "PayloadError";
    }
}

function convert(value: unknown, kind: string): any {
    if (value === null || value === undefined) {
        throw new PayloadError("null", "Payload value is null");
    }

    if (kind === "string" && typeof value === "string") {
        return value;
    }

    if (kind === "bool" && typeof value === "boolean") {
        return value;
    }

    if (["int", "uint", "float"].includes(kind) && ["number", "bigint"].includes(typeof value)) {
        if (kind === "float") {
            const result = Number(value);

            if (Number.isFinite(result)) {
                return result;
            }

            throw new PayloadError("range", "Numeric payload must be finite");
        }

        if (typeof value === "number") {
            if (!Number.isFinite(value)) {
                throw new PayloadError("range", "Numeric payload must be finite");
            }

            if (!Number.isInteger(value)) {
                throw new PayloadError("type", "Expected an integer");
            }

            if (!Number.isSafeInteger(value)) {
                throw new PayloadError("range", "Use BigInt for unsafe Number integers");
            }
        }

        const result = BigInt(value as number | bigint);
        const lower = kind === "int" ? -(1n << 63n) : 0n;
        const upper = kind === "int" ? 1n << 63n : 1n << 64n;

        if (result >= lower && result < upper) {
            return result;
        }

        throw new PayloadError("range", "Integer payload is out of range");
    }

    if (kind === "bytes" && value instanceof Uint8Array) {
        return value;
    }

    if (kind === "list" && Array.isArray(value)) {
        return value;
    }

    if (
        kind === "dict" &&
        typeof value === "object" &&
        (Object.getPrototypeOf(value) === null || Object.getPrototypeOf(value) === Object.prototype)
    ) {
        return value;
    }

    throw new PayloadError("type", `Expected ${kind} payload`);
}

export class Payload {
    constructor(
        public args: any[] = [],
        public kwargs: Dictionary = {},
    ) {}

    requireArg(index: number): any {
        if (!Number.isInteger(index) || index < 0 || index >= this.args.length) {
            throw new PayloadError("missing", `Missing positional argument ${index}`);
        }

        return this.args[index];
    }

    requireKwarg(key: string): any {
        if (!Object.hasOwn(this.kwargs, key)) {
            throw new PayloadError("missing", `Missing keyword argument ${key}`);
        }

        return this.kwargs[key];
    }

    arg(index: number): any {
        return Number.isInteger(index) && index >= 0 ? this.args[index] : undefined;
    }

    kwarg(key: string): any {
        return Object.hasOwn(this.kwargs, key) ? this.kwargs[key] : undefined;
    }

    argOr(index: number, fallback: unknown): any {
        return this.arg(index) ?? fallback;
    }

    kwargOr(key: string, fallback: unknown): any {
        return this.kwarg(key) ?? fallback;
    }

    argString(index: number): string {
        return convert(this.requireArg(index), "string");
    }

    argStringOr(index: number, fallback: string): string {
        try {
            return this.argString(index);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    argBool(index: number): boolean {
        return convert(this.requireArg(index), "bool");
    }

    argBoolOr(index: number, fallback: boolean): boolean {
        try {
            return this.argBool(index);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    argInt64(index: number): bigint {
        return convert(this.requireArg(index), "int");
    }

    argInt64Or(index: number, fallback: bigint): bigint {
        try {
            return this.argInt64(index);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    argUInt64(index: number): bigint {
        return convert(this.requireArg(index), "uint");
    }

    argUInt64Or(index: number, fallback: bigint): bigint {
        try {
            return this.argUInt64(index);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    argFloat64(index: number): number {
        return convert(this.requireArg(index), "float");
    }

    argFloat64Or(index: number, fallback: number): number {
        try {
            return this.argFloat64(index);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    argBytes(index: number): Uint8Array {
        return convert(this.requireArg(index), "bytes");
    }

    argBytesOr(index: number, fallback: Uint8Array): Uint8Array {
        try {
            return this.argBytes(index);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    argList(index: number): any[] {
        return convert(this.requireArg(index), "list");
    }

    argListOr(index: number, fallback: any[]): any[] {
        try {
            return this.argList(index);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    argDict(index: number): Dictionary {
        return convert(this.requireArg(index), "dict");
    }

    argDictOr(index: number, fallback: Dictionary): Dictionary {
        try {
            return this.argDict(index);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    kwargString(key: string): string {
        return convert(this.requireKwarg(key), "string");
    }

    kwargStringOr(key: string, fallback: string): string {
        try {
            return this.kwargString(key);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    kwargBool(key: string): boolean {
        return convert(this.requireKwarg(key), "bool");
    }

    kwargBoolOr(key: string, fallback: boolean): boolean {
        try {
            return this.kwargBool(key);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    kwargInt64(key: string): bigint {
        return convert(this.requireKwarg(key), "int");
    }

    kwargInt64Or(key: string, fallback: bigint): bigint {
        try {
            return this.kwargInt64(key);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    kwargUInt64(key: string): bigint {
        return convert(this.requireKwarg(key), "uint");
    }

    kwargUInt64Or(key: string, fallback: bigint): bigint {
        try {
            return this.kwargUInt64(key);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    kwargFloat64(key: string): number {
        return convert(this.requireKwarg(key), "float");
    }

    kwargFloat64Or(key: string, fallback: number): number {
        try {
            return this.kwargFloat64(key);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    kwargBytes(key: string): Uint8Array {
        return convert(this.requireKwarg(key), "bytes");
    }

    kwargBytesOr(key: string, fallback: Uint8Array): Uint8Array {
        try {
            return this.kwargBytes(key);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    kwargList(key: string): any[] {
        return convert(this.requireKwarg(key), "list");
    }

    kwargListOr(key: string, fallback: any[]): any[] {
        try {
            return this.kwargList(key);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }

    kwargDict(key: string): Dictionary {
        return convert(this.requireKwarg(key), "dict");
    }

    kwargDictOr(key: string, fallback: Dictionary): Dictionary {
        try {
            return this.kwargDict(key);
        } catch (error) {
            if (error instanceof PayloadError) {
                return fallback;
            }

            throw error;
        }
    }
}
