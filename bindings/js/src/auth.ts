import { getNative, initializeWasm, rustCall } from "./native.js";
import { type Dictionary, type Message, WireMessage } from "./message.js";
import type { NativeAuthenticator } from "./wasm/xconn_browser.js";

export interface Challenge extends Message {
    readonly authmethod: string;
    readonly extra: Dictionary;
}

export interface Authenticate extends Message {
    readonly signature: string;
    readonly extra: Dictionary;
}

export interface ClientAuthenticator {
    /** Release native storage. Built-in adapters recreate it lazily if reused. */
    free?(): void;

    readonly authMethod: string;
    readonly authID: string;
    readonly authExtra: object;

    authenticate(challenge: Challenge): Promise<Authenticate>;
}

abstract class RustAuthenticator implements ClientAuthenticator {
    private native?: NativeAuthenticator;
    private metadata?: object;
    abstract readonly authMethod: string;

    constructor(
        readonly authID: string,
        private readonly secret: string,
        private readonly extra: object | null,
    ) {}

    private get adapter(): NativeAuthenticator {
        return (this.native ??= rustCall(
            () => new (getNative().NativeAuthenticator)(this.authMethod, this.authID, this.secret, this.extra ?? {}),
        ));
    }

    free(): void {
        this.native?.free();
        this.native = undefined;
        this.metadata = undefined;
    }

    get authExtra(): object {
        return (this.metadata ??= rustCall(() => this.adapter.extra()));
    }

    async authenticate(challenge: Challenge): Promise<Authenticate> {
        await initializeWasm();
        return new WireMessage(rustCall(() => this.adapter.authenticate(challenge.marshal())));
    }
}

export class AnonymousAuthenticator extends RustAuthenticator {
    static readonly TYPE = "anonymous";
    readonly authMethod = AnonymousAuthenticator.TYPE;

    constructor(authID = "", extra: object | null = {}) {
        super(authID, "", extra);
    }
}

export class TicketAuthenticator extends RustAuthenticator {
    static readonly TYPE = "ticket";
    readonly authMethod = TicketAuthenticator.TYPE;

    constructor(authID: string, ticket: string, extra: object | null = {}) {
        super(authID, ticket, extra);
    }
}

export class WAMPCRAAuthenticator extends RustAuthenticator {
    static readonly TYPE = "wampcra";
    readonly authMethod = WAMPCRAAuthenticator.TYPE;

    constructor(authID: string, secret: string, extra: object | null = {}) {
        super(authID, secret, extra);
    }
}

export class CryptoSignAuthenticator extends RustAuthenticator {
    static readonly TYPE = "cryptosign";
    readonly authMethod = CryptoSignAuthenticator.TYPE;

    constructor(authID: string, key: string, extra: object | null = {}) {
        super(authID, key, extra);
    }
}
