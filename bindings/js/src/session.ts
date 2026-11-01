import { Discovery } from "./discovery.js";
import { Call } from "./call.js";
import type { StreamOptions } from "./call.js";
import { CallRequest, PublishRequest, RegisterRequest, SubscribeRequest } from "./requests.js";
import { getNative, rustCall } from "./native.js";
import { Code, URI } from "./constants.js";
import { makeMessage, inspectMessage } from "./message.js";
import type { Dictionary, WireMessage } from "./message.js";
import { ApplicationError, ProtocolError } from "./exception.js";
import { BaseSession, Result, Invocation, Event, Registration, Subscription } from "./types.js";
import type { CallControl, InvocationHandler, EventHandler } from "./types.js";
import type { RequestIds } from "./wasm/xconn_browser.js";
import { timeoutValue } from "./util.js";

interface RequestControl extends CallControl {
    opened?: (id: number) => void;
    sent?: () => void;
}

interface Pending {
    kind: number;
    expected: number;
    parse: (fields: Dictionary) => unknown;
    resolve: (value: any) => void;
    reject: (error: unknown) => void;
    cleanup: () => void;
    progress: Promise<void>;
    receiver?: CallControl["onProgress"];
    final: boolean;
    progressRequested: boolean;
}

interface Registered {
    handle: Registration;
    endpoint: InvocationHandler;
    removing?: Promise<void>;
}

interface Subscribed {
    handles: Map<Subscription, EventHandler>;
    removing?: Promise<void>;
}

export interface SessionOptions {
    closeTimeout?: number;
}

export class Session {
    private readonly ids: RequestIds;
    private readonly pending = new Map<number, Pending>();
    private readonly registrations = new Map<number, Registered>();
    private readonly subscriptions = new Map<number, Subscribed>();
    private subscriptionOperations: Promise<void> = Promise.resolve();
    private readonly invocations = new Map<number, AbortController>();
    private readonly disconnectCallbacks: Array<(reason: string) => void | Promise<void>> = [];
    private readonly errorCallbacks: Array<(error: unknown) => void> = [];
    private closed = false;
    private closing = false;
    private closePromise?: Promise<void>;
    private resolveClosed!: () => void;
    private readonly closedPromise = new Promise<void>((resolve) => {
        this.resolveClosed = resolve;
    });
    private readonly closeTimeout: number;

    constructor(
        private readonly base: BaseSession,
        options: SessionOptions = {},
    ) {
        this.closeTimeout = timeoutValue(options.closeTimeout ?? 10000, "closeTimeout");
        this.ids = new (getNative().RequestIds)();
        base.onDisconnect((reason) => this.finish(reason ?? "Connection closed"));
        void this.receiveLoop();
    }

    discovery(prefix: string): Discovery {
        return new Discovery(this, prefix);
    }

    call(procedure: string): CallRequest {
        return new CallRequest(this, procedure);
    }

    publish(topic: string): PublishRequest {
        return new PublishRequest(this, topic);
    }

    register(procedure: string, handler: InvocationHandler): RegisterRequest {
        return new RegisterRequest(this, procedure, handler);
    }

    subscribe(topic: string, handler: EventHandler): SubscribeRequest {
        return new SubscribeRequest(this, topic, handler);
    }

    get details() {
        return this.base.getSessionDetails();
    }

    isConnected(): boolean {
        return !this.closed && !this.closing && this.base.isConnected();
    }

    onDisconnect(callback: (reason: string) => void | Promise<void>): void {
        if (this.closed) {
            queueMicrotask(() => {
                void Promise.resolve()
                    .then(() => callback("Connection closed"))
                    .catch((error) => this.report(error));
            });
        } else {
            this.disconnectCallbacks.push(callback);
        }
    }

    /** Observe asynchronous event-handler and disconnect-listener failures. */
    onError(callback: (error: unknown) => void): void {
        this.errorCallbacks.push(callback);
    }

    private report(error: unknown): void {
        if (!this.errorCallbacks.length) {
            console.error(error);
            return;
        }

        for (const callback of this.errorCallbacks) {
            try {
                callback(error);
            } catch (error) {
                console.error(error);
            }
        }
    }

    private send(code: number, fields: Dictionary): void {
        this.base.send(this.base.serializer().serialize(makeMessage(code, fields)));
    }

    private settle(id: number, value: unknown, error?: unknown): void {
        const pending = this.pending.get(id);

        if (!pending) {
            return;
        }

        this.pending.delete(id);
        pending.cleanup();

        if (arguments.length === 3) {
            pending.reject(error);
        } else {
            pending.resolve(value);
        }
    }

    private cancel(id: number, error: unknown): void {
        const pending = this.pending.get(id);

        if (!pending) {
            return;
        }

        this.settle(id, undefined, error);

        if (pending.kind === Code.CALL && this.base.isConnected()) {
            try {
                this.send(Code.CANCEL, { request_id: id, options: { mode: "killnowait" } });
            } catch (error) {
                this.finish("Connection failed", error);
            }
        }
    }

    private request<T>(
        kind: number,
        expected: number,
        fields: Dictionary,
        parse: (fields: Dictionary) => T,
        control: RequestControl = {},
    ): Promise<T> {
        if (!this.isConnected()) {
            return Promise.reject(new ProtocolError("Session is closed"));
        }

        if (control.signal?.aborted) {
            return Promise.reject(control.signal.reason);
        }

        const timeout = timeoutValue(control.timeout ?? 0, "call timeout");
        const id = this.ids.next();
        return new Promise<T>((resolve, reject) => {
            let timer: ReturnType<typeof setTimeout> | undefined;
            const abort = () => this.cancel(id, control.signal?.reason);
            const cleanup = () => {
                clearTimeout(timer);
                control.signal?.removeEventListener("abort", abort);
            };
            this.pending.set(id, {
                kind,
                expected,
                parse,
                resolve,
                reject,
                cleanup,
                progress: Promise.resolve(),
                receiver: control.onProgress,
                final: false,
                progressRequested: fields.options?.receive_progress === true,
            });
            control.opened?.(id);
            control.signal?.addEventListener("abort", abort, { once: true });

            if (timeout > 0) {
                timer = setTimeout(() => this.cancel(id, new DOMException("Call timed out", "TimeoutError")), timeout);
            }

            try {
                this.send(kind, { ...fields, request_id: id });
                control.sent?.();
            } catch (error) {
                this.settle(id, undefined, error);
            }
        });
    }

    async openStream(
        procedure: string,
        args: unknown[] | null | undefined,
        kwargs: Dictionary | null | undefined,
        options: Dictionary,
        control: CallControl,
        stream: StreamOptions,
    ): Promise<Call> {
        const capacity = stream.bufferSize ?? 32;

        if (!Number.isInteger(capacity) || capacity < 1 || capacity > 65536) {
            throw new ProtocolError("result buffer size must be between 1 and 65536");
        }

        if (stream.progressiveInput !== undefined && typeof stream.progressiveInput !== "boolean") {
            throw new TypeError("progressiveInput must be a boolean");
        }

        if (stream.progressiveResults !== undefined && typeof stream.progressiveResults !== "boolean") {
            throw new TypeError("progressiveResults must be a boolean");
        }

        let requestID = 0;
        const requestOptions = {
            ...options,
            receive_progress: stream.progressiveResults ?? true,
            progress: stream.progressiveInput ?? false,
        };
        const call = new Call(
            {
                input: (args, kwargs, finish) =>
                    this.send(Code.CALL, {
                        procedure,
                        request_id: requestID,
                        args,
                        kwargs,
                        options: { ...requestOptions, progress: !finish },
                    }),
                cancel: (error) => this.cancel(requestID, error),
            },
            capacity,
            stream.progressiveInput ?? false,
        );
        let sent!: () => void;
        let failed!: (error: unknown) => void;
        const started = new Promise<void>((resolve, reject) => {
            sent = resolve;
            failed = reject;
        });
        const pending = this.request(
            Code.CALL,
            Code.RESULT,
            { procedure, args, kwargs, options: requestOptions },
            (fields) => new Result(fields.args, fields.kwargs, fields.details),
            {
                ...control,
                opened: (id) => {
                    requestID = id;
                },
                sent,
                onProgress: (result) => call.offer(result),
            },
        );
        void pending.then(
            (result) => call.complete(result),
            (error) => {
                call.fail(error);
                failed(error);
            },
        );
        await started;
        return call;
    }

    async executeCall(
        procedure: string,
        args?: unknown[] | null,
        kwargs?: Dictionary | null,
        options?: Dictionary | null,
        control: CallControl = {},
    ): Promise<Result> {
        if (options?.progress === true) {
            throw new ProtocolError("progressive input requires stream()");
        }

        const requestOptions = { ...options };

        if (control.onProgress) {
            requestOptions.receive_progress = true;
        }

        return this.request(
            Code.CALL,
            Code.RESULT,
            { procedure, args, kwargs, options: requestOptions },
            (fields) => new Result(fields.args, fields.kwargs, fields.details),
            control,
        );
    }

    callProgress(
        procedure: string,
        receiver: NonNullable<CallControl["onProgress"]>,
        args?: unknown[] | null,
        kwargs?: Dictionary | null,
        options?: Dictionary | null,
        control: CallControl = {},
    ): Promise<Result> {
        return this.executeCall(procedure, args, kwargs, options, { ...control, onProgress: receiver });
    }

    async executeRegister(
        procedure: string,
        endpoint: InvocationHandler,
        options?: Dictionary | null,
    ): Promise<Registration> {
        return this.request(Code.REGISTER, Code.REGISTERED, { procedure, options: options ?? {} }, (fields) => {
            const handle = new Registration(fields.registration_id, this);
            this.registrations.set(handle.registrationID, { handle, endpoint });
            return handle;
        });
    }

    async unregister(handle: Registration): Promise<void> {
        const entry = this.registrations.get(handle.registrationID);

        if (!entry || entry.handle !== handle) {
            return;
        }

        return (entry.removing ??= this.request(
            Code.UNREGISTER,
            Code.UNREGISTERED,
            { registration_id: handle.registrationID },
            () => {
                this.registrations.delete(handle.registrationID);
            },
        ).catch((error) => {
            entry.removing = undefined;
            throw error;
        }));
    }

    async executePublish(
        topic: string,
        args?: unknown[] | null,
        kwargs?: Dictionary | null,
        options?: Dictionary | null,
    ): Promise<void | null> {
        const fields = { topic, args, kwargs, options: options ?? {} };

        if (options?.acknowledge) {
            return this.request(Code.PUBLISH, Code.PUBLISHED, fields, () => undefined);
        }

        if (!this.isConnected()) {
            throw new ProtocolError("Session is closed");
        }

        this.send(Code.PUBLISH, { ...fields, request_id: this.ids.next() });
        return null;
    }

    async executeSubscribe(topic: string, endpoint: EventHandler, options?: Dictionary | null): Promise<Subscription> {
        return this.subscriptionOperation(() =>
            this.request(Code.SUBSCRIBE, Code.SUBSCRIBED, { topic, options: options ?? {} }, (fields) => {
                const handle = new Subscription(fields.subscription_id, this);
                let group = this.subscriptions.get(handle.subscriptionID);

                if (!group) {
                    group = { handles: new Map() };
                    this.subscriptions.set(handle.subscriptionID, group);
                }

                group.handles.set(handle, endpoint);
                return handle;
            }),
        );
    }

    async unsubscribe(handle: Subscription): Promise<void> {
        return this.subscriptionOperation(async () => {
            const group = this.subscriptions.get(handle.subscriptionID);

            if (!group?.handles.has(handle)) {
                return;
            }

            if (group.handles.size > 1) {
                group.handles.delete(handle);
                return;
            }

            return (group.removing ??= this.request(
                Code.UNSUBSCRIBE,
                Code.UNSUBSCRIBED,
                { subscription_id: handle.subscriptionID },
                () => {
                    this.subscriptions.delete(handle.subscriptionID);
                },
            ).catch((error) => {
                group.removing = undefined;
                throw error;
            }));
        });
    }

    private subscriptionOperation<T>(operation: () => Promise<T>): Promise<T> {
        const result = this.subscriptionOperations.then(operation);
        this.subscriptionOperations = result.then(
            () => undefined,
            () => undefined,
        );
        return result;
    }

    private async receiveLoop(): Promise<void> {
        try {
            while (!this.closed) {
                const message = inspectMessage(this.base.serializer().deserialize(await this.base.receive()));

                if (!this.closed) {
                    this.receive(message);
                }
            }
        } catch (error) {
            this.finish(error instanceof Error ? error.message : String(error), error);
        }
    }

    private receive(message: WireMessage): void {
        const code = message.type();
        const fields = message.fields;

        if (code === Code.GOODBYE) {
            if (!this.closing) {
                try {
                    this.send(Code.GOODBYE, { details: {}, reason: URI.CLOSE_GOOD_BYE_AND_OUT });
                } catch {
                    /* Peer may have closed already. */
                }
            }

            this.finish(fields.reason);
            return;
        }

        if (code === Code.INVOCATION) {
            void this.invoke(fields).catch((error) => this.finish("Invocation failed", error));
            return;
        }

        if (code === Code.INTERRUPT) {
            this.interrupt(fields);
            return;
        }

        if (code === Code.EVENT) {
            const group = this.subscriptions.get(fields.subscription_id);

            if (group) {
                for (const endpoint of [...group.handles.values()]) {
                    void Promise.resolve()
                        .then(() => endpoint(new Event(fields.args ?? [], fields.kwargs ?? {}, fields.details ?? {})))
                        .catch((error) => this.report(error));
                }
            }

            return;
        }

        const pending = this.pending.get(fields.request_id);

        if (!pending) {
            if (code === Code.RESULT || code === Code.ERROR || code === Code.PUBLISHED) {
                return;
            }

            throw new ProtocolError(`Unexpected WAMP response ${code}`);
        }

        if (code === Code.ERROR) {
            if (fields.message_type !== pending.kind) {
                throw new ProtocolError("ERROR does not match its request");
            }

            this.settle(
                fields.request_id,
                undefined,
                new ApplicationError(fields.uri, { args: fields.args, kwargs: fields.kwargs }),
            );
            return;
        }

        if (code !== pending.expected) {
            throw new ProtocolError("Response does not match its request");
        }

        if (code === Code.RESULT && fields.details?.progress === true) {
            if (!pending.progressRequested) {
                throw new ProtocolError("Received an unsolicited progressive result");
            }
            if (pending.final) {
                throw new ProtocolError("Progress received after final result");
            }

            if (pending.receiver) {
                const receiver = pending.receiver;
                pending.progress = pending.progress
                    .then(() => receiver(new Result(fields.args, fields.kwargs, fields.details)))
                    .catch((error) => this.cancel(fields.request_id, error));
            }

            return;
        }

        if (pending.final) {
            throw new ProtocolError("Duplicate final response");
        }

        pending.final = true;

        if (code === Code.RESULT) {
            void pending.progress
                .then(() => {
                    if (this.pending.get(fields.request_id) === pending) {
                        this.settle(fields.request_id, pending.parse(fields));
                    }
                })
                .catch((error) => this.settle(fields.request_id, undefined, error));
        } else {
            // Install callbacks synchronously before the next queued message is delivered.
            this.settle(fields.request_id, pending.parse(fields));
        }
    }

    private interrupt(fields: Dictionary): void {
        const controller = this.invocations.get(fields.request_id);
        if (controller) {
            this.invocations.delete(fields.request_id);
            controller.abort(new DOMException("Invocation cancelled", "AbortError"));
            if (fields.options?.mode === "kill") {
                this.send(Code.ERROR, {
                    message_type: Code.INVOCATION,
                    request_id: fields.request_id,
                    details: {},
                    uri: "wamp.error.canceled",
                });
            }
        }
    }

    private async invoke(fields: Dictionary): Promise<void> {
        if (this.closing || this.closed) {
            return;
        }

        const entry = this.registrations.get(fields.registration_id);

        if (!entry) {
            this.send(Code.ERROR, {
                message_type: Code.INVOCATION,
                request_id: fields.request_id,
                details: {},
                uri: URI.NO_SUCH_REGISTRATION,
            });
            return;
        }

        const controller = this.invocations.get(fields.request_id) ?? new AbortController();
        this.invocations.set(fields.request_id, controller);
        const progress = fields.details?.receive_progress
            ? (args: unknown[], kwargs: Dictionary) => {
                  this.send(Code.YIELD, { request_id: fields.request_id, options: { progress: true }, args, kwargs });
              }
            : undefined;

        try {
            const result = await entry.endpoint(
                new Invocation(
                    fields.args ?? [],
                    fields.kwargs ?? {},
                    fields.details ?? {},
                    controller.signal,
                    progress,
                ),
            );

            if (this.closed || this.closing || controller.signal.aborted) {
                return;
            }

            if (!result && fields.details?.progress === true) {
                return;
            }

            const reply = result ?? new Result();
            this.send(Code.YIELD, {
                request_id: fields.request_id,
                options: reply.details,
                args: reply.args,
                kwargs: reply.kwargs,
            });
        } catch (error) {
            if (!this.closed && !this.closing && !controller.signal.aborted) {
                try {
                    const application =
                        error instanceof ApplicationError
                            ? error
                            : new ApplicationError("wamp.error.runtime_error", { args: [String(error)] });
                    this.send(Code.ERROR, {
                        message_type: Code.INVOCATION,
                        request_id: fields.request_id,
                        details: {},
                        uri: application.message,
                        args: application.args,
                        kwargs: application.kwargs,
                    });
                } catch (error) {
                    this.finish("Invocation reply failed", error);
                }
            }
        } finally {
            if (!fields.details?.progress) {
                this.invocations.delete(fields.request_id);
            }
        }
    }

    private finish(reason: string, error: unknown = new ProtocolError(reason)): void {
        if (this.closed) {
            return;
        }

        this.closed = true;

        for (const [id] of this.pending) {
            this.settle(id, undefined, error);
        }

        for (const controller of this.invocations.values()) {
            controller.abort(error);
        }

        this.invocations.clear();
        this.registrations.clear();
        this.subscriptions.clear();
        this.ids.free();
        this.resolveClosed();

        for (const callback of this.disconnectCallbacks.splice(0)) {
            void Promise.resolve()
                .then(() => callback(reason))
                .catch((error) => this.report(error));
        }

        void this.base.close().catch((error) => this.report(error));
    }

    close(): Promise<void> {
        return (this.closePromise ??= this.closeSession());
    }

    private async closeSession(): Promise<void> {
        if (this.closed) {
            return;
        }

        this.closing = true;

        for (const [id] of this.pending) {
            this.settle(id, undefined, new ProtocolError("Session is closing"));
        }

        let timer: ReturnType<typeof setTimeout> | undefined;

        try {
            this.send(Code.GOODBYE, { details: {}, reason: URI.CLOSE_REALM });
            await Promise.race([
                this.closedPromise,
                new Promise<void>((resolve) => {
                    timer = setTimeout(resolve, this.closeTimeout);
                }),
            ]);
        } finally {
            clearTimeout(timer);
            this.finish("Session closed");
            await this.base.close();
        }
    }
}
