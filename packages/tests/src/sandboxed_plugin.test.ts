import { afterEach, describe, expect, it } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createServer } from "node:net";
import WebSocket from "ws";
import { createSandboxedCommandPlugin, RelayServer } from "@cgp/relay";
import type { RelayPlugin } from "@cgp/relay";
import { generatePrivateKey, getPublicKey, hashObject, sign, type EventBody } from "@cgp/core";

const baseDir = path.join(os.tmpdir(), "cgp-sandboxed-plugin-tests");
const relays: RelayServer[] = [];

function clean(filePath: string) {
    if (fs.existsSync(filePath)) {
        fs.rmSync(filePath, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
    }
}

async function freePort() {
    return await new Promise<number>((resolve, reject) => {
        const server = createServer();
        server.unref();
        server.once("error", reject);
        server.listen(0, "127.0.0.1", () => {
            const address = server.address();
            if (!address || typeof address === "string") {
                server.close();
                reject(new Error("Failed to allocate a test port"));
                return;
            }
            const port = address.port;
            server.close(() => resolve(port));
        });
    });
}

async function startRelay(plugins: RelayPlugin[]) {
    fs.mkdirSync(baseDir, { recursive: true });
    const port = await freePort();
    const dbPath = path.join(baseDir, `relay-${port}-${Date.now()}`);
    const relay = new RelayServer(port, dbPath, plugins, { enableDefaultPlugins: false });
    relays.push(relay);
    return { port, relay };
}

function writeScript(name: string, source: string) {
    fs.mkdirSync(baseDir, { recursive: true });
    const filePath = path.join(baseDir, name);
    fs.writeFileSync(filePath, source, "utf8");
    return filePath;
}

async function signedPublish(body: EventBody, clientEventId = `client-${Date.now()}`) {
    const privateKey = generatePrivateKey();
    const author = getPublicKey(privateKey);
    const createdAt = Date.now();
    const signature = await sign(privateKey, hashObject({ body, author, createdAt }));
    return { body, author, createdAt, signature, clientEventId };
}

async function openSocket(port: number) {
    const socket = new WebSocket(`ws://127.0.0.1:${port}`);
    await new Promise<void>((resolve, reject) => {
        socket.once("open", resolve);
        socket.once("error", reject);
    });
    return socket;
}

async function waitForFrame(socket: WebSocket, expectedKind: string, timeoutMs = 5000) {
    return await new Promise<any>((resolve, reject) => {
        const timer = setTimeout(() => {
            socket.removeListener("message", onMessage);
            reject(new Error(`Timed out waiting for ${expectedKind}`));
        }, timeoutMs);
        const onMessage = (raw: WebSocket.RawData) => {
            const [kind, payload] = JSON.parse(raw.toString());
            if (kind === expectedKind) {
                clearTimeout(timer);
                socket.removeListener("message", onMessage);
                resolve(payload);
            }
        };
        socket.on("message", onMessage);
    });
}

describe("sandboxed command relay plugins", () => {
    afterEach(async () => {
        await Promise.all(relays.splice(0).map((relay) => relay.close().catch(() => undefined)));
        clean(baseDir);
    });

    it("handles frames through a JSON-only command without exposing relay internals", async () => {
        const scriptPath = writeScript("frame-policy.mjs", `
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => input += chunk);
process.stdin.on("end", () => {
  const request = JSON.parse(input);
  if (request.protocol !== "cgp.relay.sandboxed-plugin.v1") process.exit(2);
  if (request.args?.kind === "PUBLISH" && request.args?.payload?.body?.content === "blocked") {
    process.stdout.write(JSON.stringify({
      handled: true,
      error: { code: "SANDBOX_BLOCKED", message: "blocked by sandbox policy" }
    }));
    return;
  }
  process.stdout.write(JSON.stringify({ handled: false }));
});
`);
        const plugin = createSandboxedCommandPlugin({
            name: "sandbox.frame-policy",
            command: process.execPath,
            args: [scriptPath],
            hooks: ["onFrame"],
            timeoutMs: 1000
        });
        const { port } = await startRelay([plugin]);
        const socket = await openSocket(port);
        try {
            const payload = await signedPublish({
                type: "MESSAGE",
                guildId: "sandbox-guild",
                channelId: "general",
                messageId: "blocked-message",
                content: "blocked"
            } as EventBody);
            socket.send(JSON.stringify(["PUBLISH", payload]));
            const error = await waitForFrame(socket, "ERROR");
            expect(error).toMatchObject({
                code: "SANDBOX_BLOCKED",
                message: "blocked by sandbox policy",
                clientEventId: payload.clientEventId
            });
        } finally {
            socket.close();
        }
    });

    it("fails closed when a sandboxed frame hook exceeds its timeout", async () => {
        const scriptPath = writeScript("timeout-policy.mjs", `
process.stdin.resume();
setInterval(() => {}, 1000);
`);
        const plugin = createSandboxedCommandPlugin({
            name: "sandbox.timeout-policy",
            command: process.execPath,
            args: [scriptPath],
            hooks: ["onFrame"],
            timeoutMs: 50
        });
        const { port } = await startRelay([plugin]);
        const socket = await openSocket(port);
        try {
            const payload = await signedPublish({
                type: "MESSAGE",
                guildId: "sandbox-guild",
                channelId: "general",
                messageId: "timeout-message",
                content: "hello"
            } as EventBody);
            socket.send(JSON.stringify(["PUBLISH", payload]));
            const error = await waitForFrame(socket, "ERROR");
            expect(error.code).toBe("PLUGIN_ERROR");
        } finally {
            socket.close();
        }
    });

    it("serves plugin HTTP routes through sanitized request and response objects", async () => {
        const scriptPath = writeScript("http-plugin.mjs", `
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => input += chunk);
process.stdin.on("end", () => {
  const request = JSON.parse(input);
  if (request.hook === "onHttp" && request.args?.pathname === "/plugins/sandbox.http/echo") {
    process.stdout.write(JSON.stringify({
      handled: true,
      statusCode: 202,
      headers: { "x-sandbox-hook": request.hook },
      body: {
        method: request.args.method,
        body: request.args.body,
        relayPublicKey: request.relayPublicKey ? "present" : "missing"
      }
    }));
    return;
  }
  process.stdout.write(JSON.stringify({ handled: false }));
});
`);
        const plugin = createSandboxedCommandPlugin({
            name: "sandbox.http",
            command: process.execPath,
            args: [scriptPath],
            hooks: ["onHttp"],
            timeoutMs: 1000
        });
        const { port } = await startRelay([plugin]);

        const response = await fetch(`http://127.0.0.1:${port}/plugins/sandbox.http/echo`, {
            method: "POST",
            headers: { "content-type": "text/plain" },
            body: "ping"
        });
        expect(response.status).toBe(202);
        expect(response.headers.get("x-sandbox-hook")).toBe("onHttp");
        await expect(response.json()).resolves.toMatchObject({
            method: "POST",
            body: "ping",
            relayPublicKey: "present"
        });
    });
});
