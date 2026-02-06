// patch-safari-memory-init.mjs
//
// Build-time script that analyzes WASM binaries produced by wasm-bindgen to
// extract metadata needed for the Safari shared-memory workaround.
//
// Safari 26-26.2 has a WebKit bug (stale per-thread memory size cache) that
// causes `memory.init` and `memory.grow` on shared memory to fail across
// threads. At runtime on Safari, we:
//   1. Create Memory from JS and copy data segments before instantiation
//   2. Pre-set __wasm_init_memory's atomic flag to 2 (skip its memory.init calls)
//   3. Pre-allocate memory while single-threaded (avoid memory.grow across threads)
//
// This script extracts the metadata needed for step 1 and 2: segment dest
// offsets, BSS region, and the atomic flag address.
//
// No binary modifications are made to the WASM file.
//
// Usage: node patch-safari-memory-init.mjs
//
// Inputs:  src/wasm/index_bg.wasm
// Outputs: src/wasm/memory-init-metadata.js (segment offsets + BSS + flag address)

import { readFileSync, writeFileSync } from 'fs';
import { resolve } from 'path';

const WASM_PATH = resolve('./src/wasm/index_bg.wasm');
const METADATA_PATH = resolve('./src/wasm/memory-init-metadata.js');
const METADATA_DTS_PATH = resolve('./src/wasm/memory-init-metadata.d.ts');

// WASM opcodes
const OP_I32_CONST = 0x41;
const OP_PREFIX = 0xfc;
const OP_MEMORY_INIT = 8;
const OP_MEMORY_FILL = 11;

const SECTION_CODE = 10;
const SECTION_CUSTOM = 0;

// ---- LEB128 helpers ----

function readU32LEB(buf, pos) {
  let result = 0, shift = 0, byte;
  do {
    byte = buf[pos++];
    result |= (byte & 0x7f) << shift;
    shift += 7;
  } while (byte & 0x80);
  return [result >>> 0, pos];
}

function readI32LEB(buf, pos) {
  let result = 0, shift = 0, byte;
  do {
    byte = buf[pos++];
    result |= (byte & 0x7f) << shift;
    shift += 7;
  } while (byte & 0x80);
  if (shift < 32 && (byte & 0x40)) result |= -(1 << shift);
  return [result, pos];
}

function readString(buf, pos) {
  let len;
  [len, pos] = readU32LEB(buf, pos);
  return [new TextDecoder().decode(buf.slice(pos, pos + len)), pos + len];
}

// ---- WASM structure parsing ----

function findSections(buf) {
  let pos = 8; // skip magic + version
  const sections = [];
  while (pos < buf.length) {
    const id = buf[pos++];
    const sizePos = pos;
    let size;
    [size, pos] = readU32LEB(buf, pos);
    const contentPos = pos;
    sections.push({ id, sizePos, contentPos, size });
    pos += size;
  }
  return sections;
}

function countImports(buf, sections) {
  const s = sections.find(s => s.id === 2);
  if (!s) return 0;
  let pos = s.contentPos;
  let count;
  [count, pos] = readU32LEB(buf, pos);
  let funcImports = 0;
  for (let i = 0; i < count; i++) {
    [, pos] = readString(buf, pos); // module
    [, pos] = readString(buf, pos); // name
    const kind = buf[pos++];
    if (kind === 0) {
      [, pos] = readU32LEB(buf, pos);
      funcImports++;
    } else if (kind === 1) {
      pos++; // reftype
      let flags; [flags, pos] = readU32LEB(buf, pos);
      [, pos] = readU32LEB(buf, pos);
      if (flags & 1) [, pos] = readU32LEB(buf, pos);
    } else if (kind === 2) {
      let flags; [flags, pos] = readU32LEB(buf, pos);
      [, pos] = readU32LEB(buf, pos);
      if (flags & 1) [, pos] = readU32LEB(buf, pos);
    } else if (kind === 3) {
      pos += 2;
    }
  }
  return funcImports;
}

function findFunctionByName(buf, sections, name) {
  for (const section of sections) {
    if (section.id !== SECTION_CUSTOM) continue;
    let pos = section.contentPos;
    let sectionName;
    [sectionName, pos] = readString(buf, pos);
    if (sectionName !== 'name') continue;
    const end = section.contentPos + section.size;
    while (pos < end) {
      const subId = buf[pos++];
      let subSize;
      [subSize, pos] = readU32LEB(buf, pos);
      const subEnd = pos + subSize;
      if (subId === 1) {
        let count;
        [count, pos] = readU32LEB(buf, pos);
        for (let i = 0; i < count; i++) {
          let idx;
          [idx, pos] = readU32LEB(buf, pos);
          let fname;
          [fname, pos] = readString(buf, pos);
          if (fname === name) return idx;
        }
      }
      pos = subEnd;
    }
  }
  return -1;
}

function locateFunctionBody(buf, sections, funcIndex, importCount) {
  const cs = sections.find(s => s.id === SECTION_CODE);
  if (!cs) throw new Error('No code section');
  const localIdx = funcIndex - importCount;
  let pos = cs.contentPos;
  let count;
  [count, pos] = readU32LEB(buf, pos);
  for (let i = 0; i < localIdx; i++) {
    let sz;
    [sz, pos] = readU32LEB(buf, pos);
    pos += sz;
  }
  const bodySizePos = pos;
  let bodySize;
  [bodySize, pos] = readU32LEB(buf, pos);
  return { bodySizePos, bodyStart: pos, bodySize };
}

// ---- Analyze __wasm_init_memory (extract metadata) ----

function analyzeInitMemory(buf, bodyStart, bodySize) {
  const bodyEnd = bodyStart + bodySize;
  const segments = [];
  let bss = null;
  let flagAddress = null;

  // Skip local declarations
  let pos = bodyStart;
  let localDeclCount;
  [localDeclCount, pos] = readU32LEB(buf, pos);
  for (let i = 0; i < localDeclCount; i++) {
    [, pos] = readU32LEB(buf, pos);
    pos++; // type
  }

  const constStack = [];

  while (pos < bodyEnd) {
    const opcode = buf[pos];

    if (opcode === OP_I32_CONST) {
      const startPos = pos;
      pos++;
      let value;
      [value, pos] = readI32LEB(buf, pos);
      constStack.push({ value, pos: startPos });
      // The very first i32.const is the flag address (atomic cmpxchg operand)
      if (flagAddress === null) flagAddress = value;
      continue;
    }

    if (opcode === OP_PREFIX) {
      pos++;
      let subOp;
      [subOp, pos] = readU32LEB(buf, pos);

      if (subOp === OP_MEMORY_INIT) {
        let segIdx;
        [segIdx, pos] = readU32LEB(buf, pos);
        [, pos] = readU32LEB(buf, pos); // memIdx

        if (constStack.length >= 3) {
          const len = constStack[constStack.length - 1].value;
          const dest = constStack[constStack.length - 3].value;
          segments.push({ segIdx, destOffset: dest, length: len });
        }
        console.log(`  Found memory.init seg=${segIdx} → dest=${constStack.length >= 3 ? constStack[constStack.length - 3].value : '?'} len=${constStack.length >= 3 ? constStack[constStack.length - 1].value : '?'}`);
        constStack.length = 0;
        continue;
      }
      if (subOp === OP_MEMORY_FILL) {
        [, pos] = readU32LEB(buf, pos); // memIdx
        if (constStack.length >= 3) {
          const len = constStack[constStack.length - 1].value;
          const dest = constStack[constStack.length - 3].value;
          bss = { destOffset: dest, length: len };
        }
        console.log(`  Found memory.fill (BSS) → dest=${bss?.destOffset} len=${bss?.length}`);
        constStack.length = 0;
        continue;
      }
      constStack.length = 0;
      continue;
    }

    // Skip other opcodes
    pos++;
    switch (opcode) {
      case 0x20: case 0x21: case 0x22: case 0x23: case 0x24:
      case 0x0c: case 0x0d: case 0x10:
        [, pos] = readU32LEB(buf, pos); break;
      case 0x0e: {
        let cnt; [cnt, pos] = readU32LEB(buf, pos);
        for (let i = 0; i <= cnt; i++) [, pos] = readU32LEB(buf, pos);
        break;
      }
      case 0x11:
        [, pos] = readU32LEB(buf, pos); [, pos] = readU32LEB(buf, pos); break;
      case 0x28: case 0x29: case 0x2a: case 0x2b: case 0x2c: case 0x2d:
      case 0x2e: case 0x2f: case 0x30: case 0x31: case 0x32: case 0x33:
      case 0x34: case 0x35: case 0x36: case 0x37: case 0x38: case 0x39:
      case 0x3a: case 0x3b: case 0x3c: case 0x3d: case 0x3e:
        [, pos] = readU32LEB(buf, pos); [, pos] = readU32LEB(buf, pos); break;
      case 0x3f: case 0x40:
        [, pos] = readU32LEB(buf, pos); break;
      case 0x42: while (buf[pos++] & 0x80); break;
      case 0x43: pos += 4; break;
      case 0x44: pos += 8; break;
      case 0x02: case 0x03: case 0x04: pos++; break;
      case 0xfe: {
        let aop; [aop, pos] = readU32LEB(buf, pos);
        if (aop <= 0x3e) { [, pos] = readU32LEB(buf, pos); [, pos] = readU32LEB(buf, pos); }
        break;
      }
    }
  }

  return { segments, bss, flagAddress };
}

// ---- Main ----

const buf = new Uint8Array(readFileSync(WASM_PATH));
console.log(`Reading ${WASM_PATH} (${buf.length} bytes)`);

const sections = findSections(buf);
console.log(`Found ${sections.length} sections`);

const importCount = countImports(buf, sections);
console.log(`Import count: ${importCount} functions`);

console.log('\n--- Analyzing __wasm_init_memory ---');
const initMemIdx = findFunctionByName(buf, sections, '__wasm_init_memory');
if (initMemIdx < 0) { console.error('ERROR: __wasm_init_memory not found'); process.exit(1); }
console.log(`Found __wasm_init_memory at function index ${initMemIdx}`);

const initMemBody = locateFunctionBody(buf, sections, initMemIdx, importCount);
console.log(`Function body: offset=${initMemBody.bodyStart}, size=${initMemBody.bodySize}`);

const { segments, bss, flagAddress } = analyzeInitMemory(buf, initMemBody.bodyStart, initMemBody.bodySize);
if (segments.length === 0) { console.error('ERROR: No memory.init found in __wasm_init_memory'); process.exit(1); }
if (flagAddress === null) { console.error('ERROR: Could not extract flag address from __wasm_init_memory'); process.exit(1); }
console.log(`Flag address: ${flagAddress}`);

// ---- Write metadata ----

const metadata = {
  segments: segments.map(s => ({
    segmentIndex: s.segIdx,
    destOffset: s.destOffset,
    length: s.length,
  })),
  bss: bss ? { offset: bss.destOffset, length: bss.length } : null,
  flagAddress,
};

writeFileSync(METADATA_PATH, `// Auto-generated by patch-safari-memory-init.mjs
// Memory segment initialization metadata for JS-side memory init.
// See wasm-memory-init.ts for usage.
export const MEMORY_INIT_METADATA = ${JSON.stringify(metadata, null, 2)};
`);

writeFileSync(METADATA_DTS_PATH, `// Auto-generated by patch-safari-memory-init.mjs
export declare const MEMORY_INIT_METADATA: {
  segments: Array<{
    segmentIndex: number;
    destOffset: number;
    length: number;
  }>;
  bss: { offset: number; length: number } | null;
  flagAddress: number;
};
`);

console.log(`Wrote metadata to ${METADATA_PATH}`);
console.log(`\nSegments:`);
for (const s of segments) console.log(`  seg[${s.segIdx}] → offset ${s.destOffset}, ${s.length} bytes`);
if (bss) console.log(`  BSS → offset ${bss.destOffset}, ${bss.length} bytes (zero-fill)`);
console.log(`  Flag address: ${flagAddress}`);
console.log(`\n✅ Metadata extraction complete (no binary modifications)`);
