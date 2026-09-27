import { relayIdentityStability } from "../../relay-cloudflare/src/identity";
import cloudflareWorker from "../../relay-cloudflare/src/index";
import { getPublicKey } from "@cgp/core";

describe("Cloudflare relay identity readiness", () => {
  it("reports missing or malformed signing keys as ephemeral", () => {
    expect(relayIdentityStability()).toBe("ephemeral");
    expect(relayIdentityStability("not-a-key")).toBe("ephemeral");
    expect(relayIdentityStability("ab".repeat(31))).toBe("ephemeral");
  });

  it("accepts a configured 32-byte signing key in hex", () => {
    expect(relayIdentityStability("ab".repeat(32))).toBe("configured");
    expect(relayIdentityStability(` ${"CD".repeat(32)} `)).toBe("configured");
  });

  it("keeps liveness separate from stable-identity readiness", async () => {
    const env = {} as never;
    const health = await cloudflareWorker.fetch(new Request("https://relay.test/healthz"), env);
    const readiness = await cloudflareWorker.fetch(new Request("https://relay.test/readyz"), env);
    expect(health.status).toBe(200);
    expect(readiness.status).toBe(503);
    expect(await readiness.json()).toMatchObject({ ok: false, identityStability: "ephemeral" });

    const configured = await cloudflareWorker.fetch(new Request("https://relay.test/readyz"), {
      CGP_RELAY_PRIVATE_KEY_HEX: "ab".repeat(32)
    } as never);
    expect(configured.status).toBe(200);
    expect(await configured.json()).toMatchObject({ ok: true, identityStability: "configured" });
  });

  it("publishes the configured HELLO identity without exposing its private key", async () => {
    const privateKeyHex = "ab".repeat(32);
    const privateKey = Uint8Array.from({ length: 32 }, (_, index) => Number.parseInt(privateKeyHex.slice(index * 2, index * 2 + 2), 16));
    const response = await cloudflareWorker.fetch(new Request("https://relay.test/readyz", {
      headers: { Origin: "https://hollow.example" },
    }), {
      CGP_RELAY_ID: "operator-relay-7",
      CGP_RELAY_PRIVATE_KEY_HEX: privateKeyHex,
    } as never);
    const body = await response.json() as Record<string, unknown>;

    expect(response.status).toBe(200);
    expect(body).toMatchObject({
      ok: true,
      identityStability: "configured",
      relayId: "operator-relay-7",
      relayPublicKey: getPublicKey(privateKey),
    });
    expect(body).not.toHaveProperty("privateKey");
    expect(JSON.stringify(body)).not.toContain(privateKeyHex);
    expect(response.headers.get("access-control-allow-origin")).toBe("*");
    expect(response.headers.get("access-control-allow-credentials")).toBeNull();
    expect(response.headers.get("cache-control")).toBe("no-store");
  });

  it("derives the default relay ID from the same public key as HELLO_OK", async () => {
    const privateKeyHex = "cd".repeat(32);
    const privateKey = Uint8Array.from({ length: 32 }, (_, index) => Number.parseInt(privateKeyHex.slice(index * 2, index * 2 + 2), 16));
    const response = await cloudflareWorker.fetch(new Request("https://relay.test/readyz"), {
      CGP_RELAY_PRIVATE_KEY_HEX: privateKeyHex,
    } as never);
    const body = await response.json() as Record<string, unknown>;
    const publicKey = getPublicKey(privateKey);

    expect(body.relayPublicKey).toBe(publicKey);
    expect(body.relayId).toBe(`cf-${publicKey.slice(0, 16)}`);
  });

  it("answers public GET preflights and rejects other readiness methods", async () => {
    const env = { CGP_RELAY_PRIVATE_KEY_HEX: "ef".repeat(32) } as never;
    const preflight = await cloudflareWorker.fetch(new Request("https://relay.test/readyz", {
      method: "OPTIONS",
      headers: {
        Origin: "https://hollow.example",
        "Access-Control-Request-Method": "GET",
      },
    }), env);
    expect(preflight.status).toBe(204);
    expect(preflight.headers.get("access-control-allow-origin")).toBe("*");
    expect(preflight.headers.get("access-control-allow-methods")).toBe("GET, OPTIONS");

    const rejectedPreflight = await cloudflareWorker.fetch(new Request("https://relay.test/readyz", {
      method: "OPTIONS",
      headers: { "Access-Control-Request-Method": "POST" },
    }), env);
    expect(rejectedPreflight.status).toBe(405);
    expect(rejectedPreflight.headers.get("allow")).toBe("GET, OPTIONS");

    const post = await cloudflareWorker.fetch(new Request("https://relay.test/readyz", { method: "POST" }), env);
    expect(post.status).toBe(405);
    expect(post.headers.get("access-control-allow-origin")).toBe("*");
  });
});
