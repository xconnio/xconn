import { ProtocolError } from "./exception.js";

/** Bounded host-side queue; aborting a receive removes only that waiter. */
export class Inbox<T> {
    private readonly values: Array<{ value: T; size: number }> = [];
    private readonly waiters: Array<{
        resolve: (value: T) => void;
        reject: (error: unknown) => void;
        cleanup: () => void;
    }> = [];

    private bytes = 0;
    private failure?: Error;

    constructor(
        private readonly size: (value: T) => number,
        private readonly maximumBytes: number,
        private readonly maximumItems: number,
    ) {}

    push(value: T): void {
        if (this.failure) {
            throw this.failure;
        }

        const waiter = this.waiters.shift();
        if (waiter) {
            waiter.cleanup();
            waiter.resolve(value);
            return;
        }

        const size = this.size(value);
        if (this.values.length >= this.maximumItems || this.bytes + size > this.maximumBytes) {
            throw new ProtocolError("Transport receive queue exceeded its limit");
        }

        this.values.push({ value, size });
        this.bytes += size;
    }

    receive(signal?: AbortSignal): Promise<T> {
        if (signal?.aborted) {
            return Promise.reject(signal.reason);
        }

        const next = this.values.shift();
        if (next) {
            this.bytes -= next.size;
            return Promise.resolve(next.value);
        }

        if (this.failure) {
            return Promise.reject(this.failure);
        }

        return new Promise((resolve, reject) => {
            const waiter = { resolve, reject, cleanup: () => signal?.removeEventListener("abort", abort) };
            const abort = () => {
                const index = this.waiters.indexOf(waiter);
                if (index !== -1) {
                    this.waiters.splice(index, 1);
                }

                waiter.cleanup();
                reject(signal?.reason);
            };

            signal?.addEventListener("abort", abort, { once: true });
            this.waiters.push(waiter);
        });
    }

    close(error: Error): void {
        if (this.failure) {
            return;
        }

        this.failure = error;
        this.values.length = 0;
        this.bytes = 0;
        for (const waiter of this.waiters.splice(0)) {
            waiter.cleanup();
            waiter.reject(error);
        }
    }
}
