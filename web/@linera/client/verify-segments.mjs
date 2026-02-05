// Quick diagnostic: parse the WASM data section and print segment info
// to verify the runtime parser would extract the correct data.

import { readFileSync } from 'fs';
import { resolve } from 'path';

const WASM_PATH = resolve('./src/wasm/index_bg.wasm');
const buf = readFileSync(WASM_PATH);
const bytes = new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);

function readU32LEB(buf, pos) {
  let result = 0, shift = 0, byte;
  do {
    byte = buf[pos++];
    result |= (byte & 0x7f) << shift;
    shift += 7;
  } while (byte & 0x80);
  return [result >>> 0, pos];
}

function skipInitExpr(buf, pos) {
  while (buf[pos] !== 0x0b) {
    const op = buf[pos++];
    switch (op) {
      case 0x41: case 0x23: [, pos] = readU32LEB(buf, pos); break;
      case 0x42: while (buf[pos++] & 0x80); break;
      case 0x43: pos += 4; break;
      case 0x44: pos += 8; break;
      case 0xd0: pos++; break; // ref.null
      case 0xd2: [, pos] = readU32LEB(buf, pos); break; // ref.func
    }
  }
  return pos + 1;
}

// Find data section
let pos = 8;
while (pos < bytes.length) {
  const sectionId = bytes[pos++];
  let sectionSize;
  [sectionSize, pos] = readU32LEB(bytes, pos);

  if (sectionId === 11) {
    console.log(`Data section at offset ${pos}, size ${sectionSize}`);
    let count;
    [count, pos] = readU32LEB(bytes, pos);
    console.log(`Segment count: ${count}`);

    for (let i = 0; i < count; i++) {
      let flags;
      [flags, pos] = readU32LEB(bytes, pos);

      if (flags === 0) {
        pos = skipInitExpr(bytes, pos);
      } else if (flags === 2) {
        [, pos] = readU32LEB(bytes, pos);
        pos = skipInitExpr(bytes, pos);
      }

      let dataLen;
      [dataLen, pos] = readU32LEB(bytes, pos);

      const first16 = Array.from(bytes.slice(pos, pos + 16)).map(b => b.toString(16).padStart(2, '0')).join(' ');
      const last16 = Array.from(bytes.slice(pos + dataLen - 16, pos + dataLen)).map(b => b.toString(16).padStart(2, '0')).join(' ');

      console.log(`  seg[${i}]: flags=${flags}, size=${dataLen}, offset_in_file=${pos}`);
      console.log(`    first 16 bytes: ${first16}`);
      console.log(`    last  16 bytes: ${last16}`);

      if (flags === 1) {
        // Check for all-zeros (would indicate wrong parse position)
        let nonZero = 0;
        for (let j = 0; j < Math.min(dataLen, 1000); j++) {
          if (bytes[pos + j] !== 0) nonZero++;
        }
        console.log(`    non-zero bytes in first 1000: ${nonZero}`);
      }

      pos += dataLen;
    }
    break;
  } else {
    pos += sectionSize;
  }
}
