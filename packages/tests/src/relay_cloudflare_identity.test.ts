import { relayIdentityStability } from "../../relay-cloudflare/src/identity";
import cloudflareWorker from "../../relay-cloudflare/src/index";

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
});
