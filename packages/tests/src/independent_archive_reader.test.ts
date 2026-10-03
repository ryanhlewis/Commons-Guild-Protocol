import { describe, expect, it } from "vitest";
import { computeEventId, generatePrivateKey, getPublicKey, hashObject, sign } from "@cgp/core";
import { verifyArchive } from "../../../examples/independent-archive-reader.mjs";

async function fixture() {
  const privateKey = generatePrivateKey();
  const author = getPublicKey(privateKey);
  const events: any[] = [];
  for (const body of [
    { type: "GUILD_CREATE", guildId: "interop", name: "Portable community" },
    { type: "MESSAGE", guildId: "interop", channelId: "general", content: "Hello, 世界 👋", metadata: { "2": 2, "10": 10, nested: [null, false, { z: 1, a: "quoted\"" }] } },
  ]) {
    const event: any = { seq: events.length, prevHash: events.at(-1)?.id ?? null, createdAt: 1800000000000 + events.length, author, body };
    event.signature = await sign(privateKey, hashObject({ body, author, createdAt: event.createdAt }));
    event.id = computeEventId(event);
    events.push(event);
  }
  return { archive: { format: "cgp.guild-log-export.v1", guildId: "interop", events }, expected: { count: events.length, head: events.at(-1).id } };
}

describe("independent archive reader interoperability", () => {
  it("reads core-produced signed JSON with numeric keys and Unicode using no core verifier", async () => {
    const { archive, expected } = await fixture();
    expect(verifyArchive(JSON.parse(JSON.stringify(archive)), expected)).toMatchObject({ ok: true, count: 2, messages: [{ body: { content: "Hello, 世界 👋" } }] });
  });
  it.each([
    ["content", (a: any) => { a.events[1].body.content = "forged"; }, /hash mismatch/],
    ["sequence", (a: any) => { a.events[1].seq = 8; }, /Sequence mismatch/],
    ["previous hash", (a: any) => { a.events[1].prevHash = "f".repeat(64); }, /Previous hash mismatch/],
    ["signature", (a: any) => { a.events[1].signature = "0".repeat(128); }, /signature mismatch/],
    ["truncation", (a: any) => { a.events.pop(); }, /count mismatch/],
    ["guild substitution", (a: any) => { a.guildId = "other"; }, /Guild\/body mismatch/],
    ["device authority", (a: any) => { a.events[1].deviceAuthorization = {}; }, /unsupported/],
  ])("rejects %s", async (_, mutate, reason) => {
    const { archive, expected } = await fixture();
    (mutate as Function)(archive);
    expect(() => verifyArchive(archive, expected)).toThrow(reason as RegExp);
  });
  it("requires an external checkpoint and rejects a substituted expected head", async () => {
    const { archive, expected } = await fixture();
    expect(() => verifyArchive(archive, undefined)).toThrow("expected count");
    expect(() => verifyArchive(archive, { ...expected, head: "0".repeat(64) })).toThrow("Expected head mismatch");
  });
});
