import { getNative, rustCall } from "./native.js";

export type Dictionary = Record<string, any>;

export interface Message {
    marshal(): any[];

    type(): number;
}

interface Description {
    code: number;
    wire: any[];
    fields: Dictionary;
}

/** Structural message adapter; parsing and field layouts come from Rust. */
export class WireMessage implements Message {
    constructor(private readonly description: Description) {}

    type(): number {
        return this.description.code;
    }

    marshal(): any[] {
        return this.description.wire;
    }

    get fields(): Dictionary {
        return this.description.fields;
    }

    get authmethod(): string {
        return this.fields.authmethod;
    }

    get extra(): Dictionary {
        return this.fields.extra;
    }

    get signature(): string {
        return this.fields.signature;
    }

    get requestID(): number {
        return this.fields.request_id;
    }

    get registrationID(): number {
        return this.fields.registration_id;
    }

    get subscriptionID(): number {
        return this.fields.subscription_id;
    }

    get args(): any[] {
        return this.fields.args ?? [];
    }

    get kwargs(): Dictionary {
        return this.fields.kwargs ?? {};
    }

    get details(): Dictionary {
        return this.fields.details ?? {};
    }

    get options(): Dictionary {
        return this.fields.options ?? this.fields.details ?? {};
    }

    get uri(): string {
        return this.fields.uri;
    }

    get reason(): string {
        return this.fields.reason;
    }
}

export function makeMessage(code: number, fields: Dictionary): WireMessage {
    return new WireMessage(rustCall(() => getNative().make_message(code, fields)));
}

export function inspectMessage(message: Message): WireMessage {
    return new WireMessage(rustCall(() => getNative().inspect_message(message.marshal())));
}
