export function timeoutValue(value: number, name: string): number {
    if (!Number.isFinite(value) || value < 0 || value > 2147483647) {
        throw new RangeError(`${name} is out of range`);
    }

    return value;
}

export function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
    if (signal.aborted) {
        void promise.catch(() => undefined);
        return Promise.reject(signal.reason);
    }

    return new Promise((resolve, reject) => {
        const abort = () => {
            signal.removeEventListener("abort", abort);
            reject(signal.reason);
        };
        signal.addEventListener("abort", abort, { once: true });
        promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
    });
}
