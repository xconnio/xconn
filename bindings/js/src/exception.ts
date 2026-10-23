export class ApplicationError extends Error {
    readonly args?: any[];
    readonly kwargs?: Record<string, any> | null;

    constructor(message: string, options?: { args?: unknown[]; kwargs?: Record<string, unknown> | null }) {
        super(message);
        this.name = "ApplicationError";
        this.args = options?.args;
        this.kwargs = options?.kwargs;
    }

    toString(): string {
        let text = this.message;

        if (this.args?.length) {
            text += `: ${this.args.map(String).join(", ")}`;
        }

        if (this.kwargs && Object.keys(this.kwargs).length) {
            text += `: ${Object.entries(this.kwargs)
                .map(([key, value]) => `${key}=${String(value)}`)
                .join(", ")}`;
        }

        return text;
    }
}

export class ProtocolError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "ProtocolError";
    }
}
