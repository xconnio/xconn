function check(condition, message) {
    if (!condition) throw new Error(message);
}

export function exercisePayload(api, cases) {
    const methods = {
        string: "String",
        bool: "Bool",
        int: "Int64",
        uint: "UInt64",
        float: "Float64",
        bytes: "Bytes",
        list: "List",
        dict: "Dict",
    };
    const fallback = {
        string: "fallback",
        bool: false,
        int: 7n,
        uint: 7n,
        float: 7,
        bytes: new Uint8Array(),
        list: [],
        dict: {},
    };
    for (const entry of cases) {
        let value = entry.value;
        if (["int", "uint"].includes(entry.tag)) value = BigInt(value);
        if (entry.tag === "bytes") value = Uint8Array.from(value);
        if (entry.tag === "special") value = { nan: NaN, infinity: Infinity, "-infinity": -Infinity }[value];
        for (const Type of [api.Invocation, api.Event, api.Result]) {
            const payload = new Type([value], { value });
            for (const [kind, suffix] of Object.entries(methods)) {
                for (const [scope, key] of [
                    ["arg", 0],
                    ["kwarg", "value"],
                ]) {
                    let result, error;
                    try {
                        result = payload[scope + suffix](key);
                    } catch (caught) {
                        error = caught;
                    }
                    const accepted = entry.accept.includes(kind);
                    check(
                        accepted ? !error : error instanceof api.PayloadError,
                        `${entry.name}: ${Type.name}.${scope}${suffix}`,
                    );
                    if (entry.tag === "null") check(error.kind === "null", "null was not distinguished from absence");
                    const defaulted = payload[scope + suffix + "Or"](key, fallback[kind]);
                    if (accepted) {
                        const expected =
                            kind === "float" ? Number(value) : ["int", "uint"].includes(kind) ? BigInt(value) : value;
                        check(Object.is(result, expected) && Object.is(defaulted, expected), "payload value changed");
                    } else check(defaulted === fallback[kind], "fallback changed");
                    const missing = scope === "arg" ? -1 : "absent";
                    try {
                        payload[scope + suffix](missing);
                        throw new Error("missing argument accepted");
                    } catch (error) {
                        check(error instanceof api.PayloadError && error.kind === "missing", "missing value error");
                    }
                    check(
                        payload[scope + suffix + "Or"](missing, fallback[kind]) === fallback[kind],
                        "missing fallback changed",
                    );
                }
            }
            check(Object.is(payload.requireArg(0), value), "raw lookup changed value");
        }
    }
    // Dictionary lookup must never expose inherited JavaScript properties.
    const payload = new api.Event([], {});
    check(payload.kwarg("constructor") === undefined, "inherited dictionary key leaked");
    for (const value of [9007199254740992, -9007199254740992]) {
        const event = new api.Event([value]);
        check(event.argInt64Or(0, 7n) === 7n, "unsafe Number was accepted as an integer");
    }
    return `${cases.length} shared payload cases on Invocation/Event/Result`;
}
