import type { Call, StreamOptions } from "./call.js";
import type { Session } from "./session.js";
import type { Dictionary } from "./message.js";
import type { CallControl, EventHandler, InvocationHandler, Registration, Result, Subscription } from "./types.js";

/** Configuration is local; only execute() sends an operation. */
abstract class Request<T> {
    protected readonly requestOptions: Dictionary = {};

    protected abstract run(): Promise<T>;

    execute(): Promise<T> {
        return this.run();
    }

    option(key: string, value: unknown): this {
        this.requestOptions[key] = value;
        return this;
    }

    options(values: Dictionary): this {
        Object.assign(this.requestOptions, values);
        return this;
    }
}

abstract class OutgoingRequest<T> extends Request<T> {
    protected readonly arguments: unknown[] = [];
    protected readonly keywordArguments: Dictionary = {};

    arg(value: unknown): this {
        this.arguments.push(value);
        return this;
    }

    args(...values: unknown[]): this {
        this.arguments.push(...values);
        return this;
    }

    kwarg(key: string, value: unknown): this {
        this.keywordArguments[key] = value;
        return this;
    }

    kwargs(values: Dictionary): this {
        Object.assign(this.keywordArguments, values);
        return this;
    }
}

export class CallRequest extends OutgoingRequest<Result> {
    private readonly control: CallControl = {};

    constructor(
        private readonly session: Session,
        private readonly procedure: string,
    ) {
        super();
    }

    onProgress(receiver: NonNullable<CallControl["onProgress"]>): this {
        this.control.onProgress = receiver;
        return this;
    }

    timeout(milliseconds: number): this {
        this.control.timeout = milliseconds;
        return this;
    }

    signal(signal: AbortSignal): this {
        this.control.signal = signal;
        return this;
    }

    async stream(options: StreamOptions = {}): Promise<Call> {
        if (this.control.onProgress) {
            throw new Error("streaming calls use receive() instead of callbacks");
        }

        return this.session.openStream(
            this.procedure,
            this.arguments,
            this.keywordArguments,
            this.requestOptions,
            this.control,
            options,
        );
    }

    protected run(): Promise<Result> {
        return this.session.executeCall(
            this.procedure,
            this.arguments,
            this.keywordArguments,
            this.requestOptions,
            this.control,
        );
    }
}

export class PublishRequest extends OutgoingRequest<void | null> {
    constructor(
        private readonly session: Session,
        private readonly topic: string,
    ) {
        super();
    }

    acknowledge(value = true): this {
        return this.option("acknowledge", value);
    }

    excludeMe(value = true): this {
        return this.option("exclude_me", value);
    }

    protected run(): Promise<void | null> {
        return this.session.executePublish(this.topic, this.arguments, this.keywordArguments, this.requestOptions);
    }
}

export class RegisterRequest extends Request<Registration> {
    constructor(
        private readonly session: Session,
        private readonly procedure: string,
        private readonly handler: InvocationHandler,
    ) {
        super();
    }

    match(value: string): this {
        return this.option("match", value);
    }

    invoke(value: string): this {
        return this.option("invoke", value);
    }

    protected run(): Promise<Registration> {
        return this.session.executeRegister(this.procedure, this.handler, this.requestOptions);
    }
}

export class SubscribeRequest extends Request<Subscription> {
    constructor(
        private readonly session: Session,
        private readonly topic: string,
        private readonly handler: EventHandler,
    ) {
        super();
    }

    match(value: string): this {
        return this.option("match", value);
    }

    protected run(): Promise<Subscription> {
        return this.session.executeSubscribe(this.topic, this.handler, this.requestOptions);
    }
}
