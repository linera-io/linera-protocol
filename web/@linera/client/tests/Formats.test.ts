import { expect, test } from "vitest";
import * as linera from "../dist";

// The counter example application's published formats, BCS-encoded: an empty type
// registry, `u64` operations and responses, and no messages or events. These are the
// values in examples/counter/tests/snapshots/format__format.snap. `fromBytes` parsing
// them at all is what proves the encoding matches what applications publish.
const COUNTER_FORMATS = "000c0c0202";

// BCS for the `u64` value 5: eight little-endian bytes.
const COUNTER_INCREMENT_BY_5 = "0500000000000000";

function bytes(hex: string): Uint8Array {
  return Uint8Array.from(hex.match(/../g)!.map((byte) => parseInt(byte, 16)));
}

test("Formats decodes an application's operation bytes", async () => {
  await linera.initialize();

  const formats = linera.Formats.fromBytes(bytes(COUNTER_FORMATS));
  const decoded = formats.decodeOperation(bytes(COUNTER_INCREMENT_BY_5));

  // A `u64` may render as a JavaScript number or as a string; either way it is 5.
  expect(Number(decoded)).toBe(5);
});

test("Formats rejects bytes that are not formats, and payloads that do not match", async () => {
  await linera.initialize();

  expect(() => linera.Formats.fromBytes(bytes("ffffff"))).toThrow();

  const formats = linera.Formats.fromBytes(bytes(COUNTER_FORMATS));
  // A `u64` operation needs eight bytes, not two.
  expect(() => formats.decodeOperation(bytes("0500"))).toThrow();
});
