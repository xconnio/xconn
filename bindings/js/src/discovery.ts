import type { Session } from "./session.js";
import type { CallControl } from "./types.js";

/** Connection hints and capabilities, without secret credentials. */
export interface Endpoint {
    id: string;
    addresses: string[];
    capabilities: string[];
    metadata: Record<string, unknown>;
}

/** Uses the same WAMP service contract as the native Rust discovery client. */
export class Discovery {
    constructor(
        private readonly session: Session,
        private readonly prefix: string,
    ) {}

    private async request(action: string, args: unknown[], control: CallControl): Promise<unknown> {
        const result = await this.session.executeCall(`${this.prefix}.${action}`, args, {}, {}, control);
        return result.arg(0);
    }

    async advertise(endpoint: Endpoint, control: CallControl = {}): Promise<void> {
        await this.request("advertise", [endpoint], control);
    }

    async withdraw(id: string, control: CallControl = {}): Promise<void> {
        await this.request("withdraw", [id], control);
    }

    async get(id: string, control: CallControl = {}): Promise<Endpoint> {
        return (await this.request("get", [id], control)) as Endpoint;
    }

    async list(control: CallControl = {}): Promise<Endpoint[]> {
        return (await this.request("list", [], control)) as Endpoint[];
    }
}
