// Runtime helper for JS-side WASM shared memory initialization.
//
// Works around a Safari/WebKit bug where the `memory.init` WASM instruction
// traps when targeting shared memory (SharedArrayBuffer). Instead, this module
// parses the passive data segments directly from the WASM binary and copies
// them into the SharedArrayBuffer from JavaScript before WASM instantiation.
//
// The WASM binary must be pre-patched by patch-safari-memory-init.mjs which
// replaces memory.init/memory.fill/data.drop with drops/nops.

import { MEMORY_INIT_METADATA } from './wasm/memory-init-metadata.js';

// WASM data section ID
const SECTION_DATA = 11;

// Passive segment flag
const SEG_PASSIVE = 1;

function readU32LEB(buf: Uint8Array, pos: number): [number, number] {
  let result = 0;
  let shift = 0;
  let byte: number;
  do {
    byte = buf[pos++];
    result |= (byte & 0x7f) << shift;
    shift += 7;
  } while (byte & 0x80);
  return [result >>> 0, pos];
}

function skipInitExpr(buf: Uint8Array, pos: number): number {
  // Init expressions are terminated by 0x0b (end)
  while (buf[pos] !== 0x0b) {
    const opcode = buf[pos++];
    switch (opcode) {
      case 0x41: // i32.const
      case 0x23: // global.get
        [, pos] = readU32LEB(buf, pos);
        break;
      case 0x42: // i64.const
        // skip LEB128 i64
        while (buf[pos++] & 0x80);
        break;
      case 0x43: // f32.const
        pos += 4;
        break;
      case 0x44: // f64.const
        pos += 8;
        break;
      default:
        // Other opcodes in init exprs are rare; skip LEB if needed
        break;
    }
  }
  return pos + 1; // skip the 0x0b end byte
}

/**
 * Parse passive data segments from a WASM binary's data section.
 * Returns a map from segment index to the raw segment bytes.
 */
function parsePassiveSegments(wasmBytes: ArrayBuffer): Map<number, Uint8Array> {
  const buf = new Uint8Array(wasmBytes);
  let pos = 8; // skip magic + version

  // Find the data section
  while (pos < buf.length) {
    const sectionId = buf[pos++];
    let sectionSize: number;
    [sectionSize, pos] = readU32LEB(buf, pos);

    if (sectionId !== SECTION_DATA) {
      pos += sectionSize;
      continue;
    }

    // Parse data section entries
    const segments = new Map<number, Uint8Array>();
    let count: number;
    [count, pos] = readU32LEB(buf, pos);

    for (let i = 0; i < count; i++) {
      let flags: number;
      [flags, pos] = readU32LEB(buf, pos);

      if (flags === 0) {
        // Active segment (memory 0): init_expr + data
        pos = skipInitExpr(buf, pos);
      } else if (flags === 2) {
        // Active segment (memory N): memidx + init_expr + data
        [, pos] = readU32LEB(buf, pos); // memidx
        pos = skipInitExpr(buf, pos);
      }
      // flags === 1 means passive — no init expr, just data

      let dataLen: number;
      [dataLen, pos] = readU32LEB(buf, pos);

      if (flags === SEG_PASSIVE) {
        // Copy the segment data (don't hold a reference into the WASM ArrayBuffer
        // which may be detached during compilation)
        segments.set(i, buf.slice(pos, pos + dataLen));
      }
      pos += dataLen;
    }
    return segments;
  }

  throw new Error('No data section found in WASM binary');
}

/**
 * Initialize shared memory with passive data segments from the WASM binary.
 * Must be called before __wbindgen_start so that __wasm_init_memory (which has
 * its memory.init instructions patched out) finds the memory already populated.
 */
export function initializeSharedMemory(
  memory: WebAssembly.Memory,
  wasmBytes: ArrayBuffer,
): void {
  const segments = parsePassiveSegments(wasmBytes);
  const memoryView = new Uint8Array(memory.buffer);

  for (const seg of MEMORY_INIT_METADATA.segments) {
    const data = segments.get(seg.segmentIndex);
    if (!data) {
      throw new Error(
        `Passive segment ${seg.segmentIndex} not found in WASM binary`,
      );
    }
    if (data.length !== seg.length) {
      throw new Error(
        `Segment ${seg.segmentIndex} size mismatch: expected ${seg.length}, got ${data.length}`,
      );
    }
    memoryView.set(data, seg.destOffset);
  }

  // Zero-fill BSS region (SharedArrayBuffer is already zero-initialized on
  // creation, but we do this explicitly for correctness in case the memory
  // was reused or grown)
  if (MEMORY_INIT_METADATA.bss) {
    const { offset, length } = MEMORY_INIT_METADATA.bss;
    memoryView.fill(0, offset, offset + length);
  }
}
