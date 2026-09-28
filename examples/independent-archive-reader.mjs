// Deliberately imports no CGP implementation. Shared dependency: secp256k1 math.
import { createHash } from "node:crypto";
import { readFileSync, statSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { verify } from "@noble/secp256k1";

// JSON values only. Build strings directly so integer-looking object keys also
// sort lexically, matching the CGP canonicalization rather than JS enumeration.
function canonical(value, depth = 0) {
  if (depth > 100) throw new Error("JSON nesting exceeds reader limit");
  if (value === null || typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number" && Number.isFinite(value)) return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((item) => canonical(item, depth + 1)).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key], depth + 1)}`).join(",")}}`;
  throw new Error("Non-JSON value");
}
const digest = (value) => createHash("sha256").update(canonical(value), "utf8").digest("hex");
const require = (condition, reason) => { if (!condition) throw new Error(reason); };

export function verifyArchive(archive, expected) {
  require(archive?.format === "cgp.guild-log-export.v1", "Unsupported archive format");
  require(typeof archive.guildId === "string" && archive.guildId.length > 0, "Missing guild identity");
  require(Array.isArray(archive.events) && archive.events.length > 0, "Archive must contain a complete nonempty log");
  require(Number.isSafeInteger(expected?.count) && expected.count > 0, "Trusted expected count is required");
  require(typeof expected?.head === "string" && /^[0-9a-f]{64}$/.test(expected.head), "Trusted expected head is required");
  require(archive.events.length === expected.count, "Expected event count mismatch (possible truncation)");
  let previous = null;
  const messages = [];
  for (const [index, event] of archive.events.entries()) {
    require(event && event.seq === index, `Sequence mismatch at ${index}`);
    require(event.prevHash === previous, `Previous hash mismatch at ${index}`);
    require(Number.isSafeInteger(event.createdAt) && event.createdAt > 0, `Invalid creation time at ${index}`);
    require(event.body && typeof event.body.type === "string" && event.body.guildId === archive.guildId, `Guild/body mismatch at ${index}`);
    require(index !== 0 || event.body.type === "GUILD_CREATE", "Genesis must create the guild");
    // Device authorization is a stateful protocol; fail closed instead of giving
    // a misleading full-verification result from the device signature alone.
    require(event.deviceAuthorization == null, `Delegated-device authorization unsupported at ${index}`);
    const unsigned = { seq: event.seq, prevHash: event.prevHash, createdAt: event.createdAt, author: event.author, body: event.body };
    if (Object.hasOwn(event, "deviceAuthorization")) unsigned.deviceAuthorization = event.deviceAuthorization;
    require(event.id === digest(unsigned), `Event hash mismatch at ${index}`);
    const signed = digest({ body: event.body, author: event.author, createdAt: event.createdAt });
    let valid = false;
    try { valid = verify(event.signature, signed, event.author); } catch { /* rejected below */ }
    require(valid, `Author signature mismatch at ${index}`);
    previous = event.id;
    if (event.body.type === "MESSAGE") messages.push({ seq: index, author: event.author, body: event.body });
  }
  require(previous === expected.head, "Expected head mismatch");
  return { ok: true, guildId: archive.guildId, count: archive.events.length, head: previous, messages };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    const [, , file, expectedHead, expectedCount, mode] = process.argv;
    require(Boolean(file), "Usage: node examples/independent-archive-reader.mjs ARCHIVE EXPECTED_HEAD EXPECTED_COUNT [--read]");
    require(statSync(file).size <= 32 * 1024 * 1024, "Archive exceeds 32 MiB reader limit");
    const result = verifyArchive(JSON.parse(readFileSync(file, "utf8")), { head: expectedHead, count: Number(expectedCount) });
    if (mode !== "--read") delete result.messages;
    console.log(JSON.stringify(result, null, 2));
  } catch (error) {
    console.error(JSON.stringify({ ok: false, error: error instanceof Error ? error.message : String(error) }));
    process.exitCode = 1;
  }
}
