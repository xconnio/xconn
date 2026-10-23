import { readFile, mkdtemp, rm } from "node:fs/promises";
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { createServer } from "node:http";
import path from "node:path";
import os from "node:os";
import { pathToFileURL } from "node:url";
import { exerciseStreaming } from "./streaming.mjs";
import { exerciseStreamingInterop } from "./streaming-interop.mjs";
import { exercise } from "./browser.mjs";
import { exerciseEdges, exerciseQueue } from "./edge.mjs";
import { exercisePayload } from "./payload.mjs";
import { exerciseDiscovery } from "./discovery.mjs";
import { exerciseCrypto } from "./crypto.mjs";

const root = path.resolve(import.meta.dirname, "../../..");
const artifacts = path.resolve(process.argv[2] ?? path.join(root, "bindings/js/dist"));
const payloadCases = JSON.parse(await readFile(path.join(root, "tests/payload/cases.json"), "utf8"));
const cryptoFixture = JSON.parse(await readFile(path.join(root, "tests/crypto/go-reference.json"), "utf8"));
const variant = process.argv[3] ?? "full";
const children = [];
let server;
let profile;

async function startReference() {
    const binary = process.env.XCONN_REFERENCE_GO_BIN;
    if (!binary) return undefined;
    const child = spawn(binary, [], { stdio: ["ignore", "pipe", "inherit"] });
    children.push(child);
    const lines = createInterface({ input: child.stdout });
    for await (const line of lines) {
        lines.close();
        return JSON.parse(line).ws;
    }
    throw new Error("reference router did not announce its address");
}

async function startDiscovery() {
    const binary = process.env.XCONN_DISCOVERY_BIN;
    if (!binary) return undefined;
    const child = spawn(binary, [], { stdio: ["ignore", "pipe", "inherit"] });
    children.push(child);
    const lines = createInterface({ input: child.stdout });
    for await (const line of lines) {
        lines.close();
        return line;
    }
    throw new Error("discovery host did not announce its address");
}

async function startRustPeer() {
    const binary = process.env.XCONN_STREAM_RUST_BIN;
    if (!binary) return undefined;
    const child = spawn(binary, [], { stdio: ["pipe", "pipe", "inherit"] });
    children.push(child);
    const lines = createInterface({ input: child.stdout });
    for await (const line of lines) {
        lines.close();
        return JSON.parse(line).ws;
    }
    throw new Error("Rust streaming peer did not announce its address");
}

async function browser(uri, discoveryUri, rustURI) {
    const executable = process.env.CHROMIUM_BIN;
    if (!executable) return [];
    server = createServer(async (request, response) => {
        try {
            const url = new URL(request.url, "http://localhost");
            if (url.pathname === "/") {
                response.setHeader("Content-Type", "text/html");
                response.end("<!doctype html><title>XConn browser integration tests</title>");
                return;
            }
            const testFile = [
                "/browser.mjs",
                "/edge.mjs",
                "/payload.mjs",
                "/discovery.mjs",
                "/crypto.mjs",
                "/streaming.mjs",
                "/streaming-interop.mjs",
            ].includes(url.pathname);
            const file = testFile
                ? path.join(import.meta.dirname, url.pathname.slice(1))
                : path.resolve(artifacts, `.${url.pathname}`);
            if (!testFile && !file.startsWith(artifacts + path.sep)) {
                throw new Error("invalid path");
            }
            response.setHeader("Content-Type", file.endsWith(".wasm") ? "application/wasm" : "text/javascript");
            response.end(await readFile(file));
        } catch {
            response.writeHead(404).end();
        }
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const base = `http://127.0.0.1:${server.address().port}`;
    profile = await mkdtemp(path.join(os.tmpdir(), "xconn-wasm-browser-"));
    const chrome = spawn(
        executable,
        [
            "--headless",
            "--no-sandbox",
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--remote-debugging-port=0",
            `--user-data-dir=${profile}`,
        ],
        { stdio: ["ignore", "ignore", "pipe"] },
    );
    children.push(chrome);
    const lines = createInterface({ input: chrome.stderr });
    let endpoint;
    for await (const line of lines) {
        if (line.includes("DevTools listening on ")) {
            endpoint = line.split("DevTools listening on ")[1];
            lines.close();
            break;
        }
    }
    if (!endpoint) throw new Error("Chromium did not expose DevTools");
    const socket = new WebSocket(endpoint);
    await new Promise((resolve, reject) => {
        socket.addEventListener("open", resolve, { once: true });
        socket.addEventListener("error", reject, { once: true });
    });
    const exceptions = [];
    let sequence = 0;
    const pending = new Map();
    const events = new Map();
    socket.addEventListener("close", () => {
        for (const waiter of pending.values()) waiter.reject(new Error("Chromium DevTools connection closed"));
        pending.clear();
    });
    socket.addEventListener("message", ({ data }) => {
        const message = JSON.parse(data);
        if (message.id) {
            const waiter = pending.get(message.id);
            pending.delete(message.id);
            if (message.error) waiter.reject(new Error(JSON.stringify(message.error)));
            else waiter.resolve(message.result);
        } else {
            if (message.method === "Runtime.exceptionThrown") exceptions.push(message.params.exceptionDetails);
            events.get(`${message.sessionId}:${message.method}`)?.(message.params);
        }
    });
    const call = (method, params = {}, sessionId) =>
        new Promise((resolve, reject) => {
            const id = ++sequence;
            pending.set(id, { resolve, reject });
            socket.send(JSON.stringify({ id, method, params, sessionId }));
        });
    const { targetId } = await call("Target.createTarget", { url: "about:blank" });
    const { sessionId } = await call("Target.attachToTarget", { targetId, flatten: true });
    await call("Page.enable", {}, sessionId);
    await call("Runtime.enable", {}, sessionId);
    const loaded = new Promise((resolve) => events.set(`${sessionId}:Page.loadEventFired`, resolve));
    await call("Page.navigate", { url: base }, sessionId);
    await loaded;
    const expression = `(async () => {
        const module = await import(${JSON.stringify(`${base}/index.js`)});
        await module.initializeWasm();
        const {exercise} = await import(${JSON.stringify(`${base}/browser.mjs`)});
        const result = await exercise(module, ${JSON.stringify(variant)}, ${JSON.stringify(uri)});
        const {exerciseEdges, exerciseQueue} = await import(${JSON.stringify(`${base}/edge.mjs`)});
        const {SocketInbox} = await import(${JSON.stringify(`${base}/transport.js`)});
        result.queue = await exerciseQueue(SocketInbox);
        result.edges = await exerciseEdges(module, ${JSON.stringify(uri)});
        const {exercisePayload} = await import(${JSON.stringify(`${base}/payload.mjs`)});
        result.payload = exercisePayload(module, ${JSON.stringify(payloadCases)});
        const {exerciseStreaming} = await import(${JSON.stringify(`${base}/streaming.mjs`)});
        result.streaming = await exerciseStreaming(module, ${JSON.stringify(uri)});
        const {exerciseStreamingInterop} = await import(${JSON.stringify(`${base}/streaming-interop.mjs`)});
        result.interop = await exerciseStreamingInterop(module, ${JSON.stringify(rustURI)}, ${JSON.stringify(variant)});
        if (${JSON.stringify(variant)} === "full") {
            const {exerciseCrypto} = await import(${JSON.stringify(`${base}/crypto.mjs`)});
            result.crypto = exerciseCrypto(module, ${JSON.stringify(cryptoFixture)});
        }
        if (${JSON.stringify(discoveryUri)} !== undefined) {
            const {exerciseDiscovery} = await import(${JSON.stringify(`${base}/discovery.mjs`)});
            result.discovery = await exerciseDiscovery(module, ${JSON.stringify(discoveryUri)});
        }
        return result;
    })()`;
    const result = await call("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true }, sessionId);
    if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
    await call(
        "Runtime.evaluate",
        { expression: "new Promise(resolve => setTimeout(resolve, 0))", awaitPromise: true },
        sessionId,
    );
    if (exceptions.length) throw new Error(JSON.stringify(exceptions));
    const results = [result.result.value];
    await call("Browser.close");
    socket.close();
    return results;
}

const deadline = setTimeout(() => {
    console.error("Browser client test timed out");
    process.exitCode = 1;
    for (const child of children) child.kill();
}, 60_000);
try {
    const uri = await startReference();
    const discoveryUri = await startDiscovery();
    const rustURI = await startRustPeer();

    const module = await import(pathToFileURL(path.join(artifacts, "index.js")));
    await module.initializeWasm(await readFile(path.join(artifacts, "wasm/xconn_browser_bg.wasm")));
    const result = await exercise(module, variant, uri);
    result.edges = await exerciseEdges(module, uri);
    const { SocketInbox } = await import(pathToFileURL(path.join(artifacts, "transport.js")));
    result.queue = await exerciseQueue(SocketInbox);
    result.payload = exercisePayload(module, payloadCases);
    result.streaming = await exerciseStreaming(module, uri);
    result.interop = await exerciseStreamingInterop(module, rustURI, variant);
    if (variant === "full") result.crypto = exerciseCrypto(module, cryptoFixture);
    if (discoveryUri) result.discovery = await exerciseDiscovery(module, discoveryUri);
    console.log("Node:", result);
    for (const result of await browser(uri, discoveryUri, rustURI)) console.log("Chromium:", result);
} finally {
    clearTimeout(deadline);
    for (const child of children) {
        const exited = new Promise((resolve) => child.once("exit", resolve));
        if (child.exitCode === null && child.signalCode === null) {
            child.kill();
            await exited;
        }
    }
    if (server) await new Promise((resolve) => server.close(resolve));
    if (profile) await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
