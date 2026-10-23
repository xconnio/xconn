import init, * as native from "./wasm/xconn_browser.js";
import type { InitInput } from "./wasm/xconn_browser.js";
import { ProtocolError } from "./exception.js";

let initialization: Promise<void> | undefined;

let ready = false;

/** Client.connect initializes automatically. Await this before using synchronous codec/auth APIs directly. */
export function initializeWasm(input?: InitInput | Promise<InitInput>): Promise<void> {
    if (!initialization) {
        initialization = init(input === undefined ? undefined : { module_or_path: input })
            .then(() => {
                ready = true;
            })
            .catch((error) => {
                initialization = undefined;
                throw error;
            });
    }

    return initialization;
}

export function getNative(): typeof native {
    if (!ready) {
        throw new ProtocolError("Await initializeWasm() before using synchronous protocol APIs");
    }

    return native;
}

export function rustCall<T>(action: () => T): T {
    try {
        return action();
    } catch (error) {
        if (error instanceof Error) {
            throw error;
        }

        throw new ProtocolError(String(error));
    }
}
