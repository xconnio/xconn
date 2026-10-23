import { Payload as PayloadAccess } from "./payload.js";
import type { Session } from "./session.js";
import type { Serializer, Payload } from "./serializers.js";
import type { Dictionary, Message } from "./message.js";

export class SessionDetails {
    constructor(
        readonly sessionID: number,
        readonly realm: string,
        readonly authid: string,
        readonly authrole: string,
        readonly authmethod?: string,
        readonly authextra?: Dictionary,
        readonly roles?: Dictionary,
    ) {}
}

export abstract class BaseSession {
    id(): number {
        return this.getSessionDetails().sessionID;
    }

    realm(): string {
        return this.getSessionDetails().realm;
    }

    authid(): string {
        return this.getSessionDetails().authid;
    }

    authrole(): string {
        return this.getSessionDetails().authrole;
    }

    serializer(): Serializer {
        throw new Error("UnimplementedError");
    }

    send(_data: Payload): void {
        throw new Error("UnimplementedError");
    }

    async receive(): Promise<Payload> {
        throw new Error("UnimplementedError");
    }

    sendMessage(message: Message): void {
        this.send(this.serializer().serialize(message));
    }

    async receiveMessage(): Promise<Message> {
        return this.serializer().deserialize(await this.receive());
    }

    async close(): Promise<void> {
        throw new Error("UnimplementedError");
    }

    isConnected(): boolean {
        throw new Error("UnimplementedError");
    }

    onDisconnect(_callback: (reason?: string) => void | Promise<void>): void {
        throw new Error("UnimplementedError");
    }

    getSessionDetails(): SessionDetails {
        throw new Error("UnimplementedError");
    }
}

export class Result extends PayloadAccess {
    get progress(): boolean {
        return this.details.progress === true;
    }

    static create(...args: unknown[]): Result {
        return new Result(args);
    }

    details: Dictionary;

    constructor(args?: unknown[] | null, kwargs?: Dictionary | null, details?: Dictionary | null) {
        super(args ?? [], kwargs ?? {});
        this.details = details ?? {};
    }
}

export class Invocation extends PayloadAccess {
    constructor(
        args: unknown[] = [],
        kwargs: Dictionary = {},
        readonly details: Dictionary = {},
        readonly signal: AbortSignal = new AbortController().signal,
        private readonly progress?: (args: unknown[], kwargs: Dictionary) => void,
    ) {
        super(args, kwargs);
    }

    async sendProgress(args: unknown[] = [], kwargs: Dictionary = {}): Promise<void> {
        this.signal.throwIfAborted();

        if (!this.progress) {
            throw new Error("Progress was not requested by the caller");
        }

        this.progress(args, kwargs);
    }
}

export class Event extends PayloadAccess {
    constructor(
        args: unknown[] = [],
        kwargs: Dictionary = {},
        readonly details: Dictionary = {},
    ) {
        super(args, kwargs);
    }
}

export class Registration {
    constructor(
        readonly registrationID: number,
        private readonly session: Session,
    ) {}

    unregister(): Promise<void> {
        return this.session.unregister(this);
    }
}

export class Subscription {
    constructor(
        readonly subscriptionID: number,
        private readonly session: Session,
    ) {}

    unsubscribe(): Promise<void> {
        return this.session.unsubscribe(this);
    }
}

export type InvocationHandler = (invocation: Invocation) => Result | void | null | Promise<Result | void | null>;

export type EventHandler = (event: Event) => void | Promise<void>;

export interface CallControl {
    signal?: AbortSignal;
    timeout?: number;
    onProgress?: (result: Result) => void | Promise<void>;
}
