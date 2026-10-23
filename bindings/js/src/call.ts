import { ProtocolError } from "./exception.js";
import type { Result } from "./types.js";
import type { Dictionary } from "./message.js";

export interface StreamOptions {
    progressiveInput?: boolean;
    progressiveResults?: boolean;
    bufferSize?: number;
}

interface Waiter {
    resolve: (result: Result | null) => void;
    reject: (error: unknown) => void;
}

export interface CallTransport {
    input: (args: unknown[], kwargs: Dictionary, finish: boolean) => void;
    cancel: (error: unknown) => void;
}

/** One owned RPC with independent input and result paths. */
export class Call implements AsyncIterable<Result> {
    private readonly queue: Result[] = [];
    private waiter?: Waiter;
    private receiving = false;
    private done = false;
    private inputFinished: boolean;
    private failure: unknown;
    private failed = false;

    constructor(
        private readonly transport: CallTransport,
        private readonly capacity: number,
        progressiveInput: boolean,
    ) {
        this.inputFinished = !progressiveInput;
    }

    /** @internal */
    offer(result: Result): void {
        if (this.done) {
            return;
        }

        if (this.waiter) {
            const waiter = this.waiter;
            this.waiter = undefined;
            waiter.resolve(result);
        } else if (this.queue.length < this.capacity) {
            this.queue.push(result);
        } else {
            const error = new ProtocolError("call result queue overflow");
            this.fail(error);
            this.transport.cancel(error);
            throw error;
        }
    }

    /** @internal */
    complete(result: Result): void {
        if (this.done) {
            return;
        }

        try {
            this.offer(result);
            this.done = true;
        } catch {
            /* offer records overflow and cancels correlation */
        }
    }

    /** @internal */
    fail(error: unknown): void {
        if (this.done) {
            return;
        }

        this.done = true;

        if (this.waiter) {
            const waiter = this.waiter;
            this.waiter = undefined;
            waiter.reject(error);
        } else {
            this.failure = error;
            this.failed = true;
        }
    }

    async receive(): Promise<Result | null> {
        if (this.receiving) {
            throw new ProtocolError("another receive is already pending");
        }

        this.receiving = true;

        try {
            if (this.queue.length) {
                return this.queue.shift()!;
            }

            if (this.failed) {
                this.failed = false;
                throw this.failure;
            }

            if (this.done) {
                return null;
            }

            return await new Promise<Result | null>((resolve, reject) => {
                this.waiter = { resolve, reject };
            });
        } finally {
            this.receiving = false;
        }
    }

    async send(args: unknown[] = [], kwargs: Dictionary = {}): Promise<void> {
        this.input(args, kwargs, false);
    }

    async finish(args: unknown[] = [], kwargs: Dictionary = {}): Promise<void> {
        this.input(args, kwargs, true);
    }

    private input(args: unknown[], kwargs: Dictionary, finish: boolean): void {
        if (this.done) {
            throw new ProtocolError("call is finished");
        }

        if (this.inputFinished) {
            throw new ProtocolError("progressive input is disabled or already finished");
        }

        try {
            this.transport.input(args, kwargs, finish);
        } catch (error) {
            this.fail(error);
            this.transport.cancel(error);
            throw error;
        }

        if (finish) {
            this.inputFinished = true;
        }
    }

    async cancel(): Promise<void> {
        if (this.done) {
            return;
        }

        const error = new DOMException("Call cancelled", "AbortError");
        this.queue.length = 0;
        this.fail(error);
        this.transport.cancel(error);
    }

    async close(): Promise<void> {
        await this.cancel();
    }

    results(): AsyncIterableIterator<Result> {
        return {
            next: async () => {
                const result = await this.receive();
                return result === null ? { done: true, value: undefined } : { done: false, value: result };
            },
            return: async () => {
                await this.close();
                return { done: true, value: undefined };
            },
            [Symbol.asyncIterator]() {
                return this;
            },
        };
    }

    [Symbol.asyncIterator](): AsyncIterableIterator<Result> {
        return this.results();
    }
}
