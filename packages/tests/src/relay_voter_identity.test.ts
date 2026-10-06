import { describe, expect, it } from "vitest";
import { normalizeRelayWriteQuorumConfig } from "@cgp/relay/src/write_quorum";
import { normalizeRelaySequencerConsensusConfig } from "@cgp/relay/src/sequencer_consensus";

// SEC1 encodings of the same secp256k1 generator (private scalar 1).
const compressed = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";
const uncompressed = "0479be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798483ada7726a3c4655da4fbfc0e1108a8fd17b448a68554199c47d08ffb10d4b8";
const second = "02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5";
const third = "02f9308a019258c31049344f85f89d5229b531c845836f99b08601f113bce036f9";

for (const [name, normalize] of [
  ["write quorum", normalizeRelayWriteQuorumConfig],
  ["sequencer", normalizeRelaySequencerConsensusConfig],
] as const) {
  describe(`${name} voter identity`, () => {
    it("rejects two SEC1 encodings of one signer instead of counting two voters", () => {
      expect(() => normalize({ epoch: "test", members: [compressed, uncompressed] })).toThrow(/compressed/i);
    });
    it("deduplicates uppercase and whitespace aliases before computing majority", () => {
      const config = normalize({ epoch: "test", members: [compressed, ` ${compressed.toUpperCase()} `, second, third], requiredVotes: 1 });
      expect(config.members).toEqual([compressed, second, third]);
      expect(config.requiredVotes).toBe(2);
      expect(() => normalize({ epoch: "test", members: [compressed, compressed.toUpperCase()] })).toThrow(/at least two/);
    });
    it.each(["", "signer", "04" + "a".repeat(64), "02" + "z".repeat(64)])("rejects malformed voter %s", (member) => {
      expect(() => normalize({ epoch: "test", members: [compressed, second, member] })).toThrow(/compressed/i);
    });
    it("requires both members in a two-member configuration", () => {
      expect(normalize({ epoch: "test", members: [compressed, second], requiredVotes: 1 }).requiredVotes).toBe(2);
    });
  });
}
