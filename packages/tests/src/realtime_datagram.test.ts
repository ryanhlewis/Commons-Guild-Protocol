import { describe, expect, it } from "vitest";
import {
  RealtimeDatagramReassembler,
  fragmentRealtimeDatagram,
} from "@cgp/core";

describe("realtime datagram framing", () => {
  it("reassembles fragmented frames received out of order", () => {
    const frame = Uint8Array.from({ length: 8_192 }, (_, index) => index % 251);
    const fragments = fragmentRealtimeDatagram(frame, 42, 500);
    const reassembler = new RealtimeDatagramReassembler();
    let result: Uint8Array | undefined;

    for (const fragment of fragments.reverse()) {
      result = reassembler.push(fragment) ?? result;
    }

    expect(fragments.length).toBeGreaterThan(1);
    expect(result).toEqual(frame);
  });

  it("drops incomplete and oversized assemblies", () => {
    const reassembler = new RealtimeDatagramReassembler({
      maxFrameBytes: 32,
      maxAssemblyAgeMs: 10,
    });
    const fragments = fragmentRealtimeDatagram(new Uint8Array(64), 7, 32);

    expect(reassembler.push(fragments[0], 0)).toBeUndefined();
    expect(reassembler.push(fragments[1], 20)).toBeUndefined();
    expect(reassembler.push(fragments[2], 20)).toBeUndefined();
  });
});
