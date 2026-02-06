// patch-safari-memory-init.mjs
//
// Build-time script that patches WASM binaries produced by wasm-bindgen to work
// around Safari/WebKit bugs with shared memory:
//
//   Bug 1: `memory.init` on shared memory traps (crashes the module).
//   Bug 2: `memory.grow` on shared memory crashes during multi-threaded
//          operation (WebKit #304386).
//
// Two-phase approach:
//
//   Phase 1 — ANALYZE `__wasm_init_memory` (no patching):
//     Extracts segment dest offsets, BSS info, and the atomic flag address.
//     At runtime, Safari skips this function by pre-setting the flag to 2
//     via Atomics.store, then copies segments from JS before instantiation.
//
//   Phase 2 — PATCH `__wasm_init_tls`:
//     Replaces `memory.init $.tdata` with `memory.copy` so that per-thread
//     TLS initialization reads the template from linear memory instead of
//     from a passive segment. This is the only binary modification.
//
// Usage: node patch-safari-memory-init.mjs
//
// Inputs:  src/wasm/index_bg.wasm
// Outputs: src/wasm/index_bg.wasm (patched: __wasm_init_tls only)
//          src/wasm/memory-init-metadata.js (segment offsets + BSS + flag address)

import { readFileSync, writeFileSync } from 'fs';
import { resolve } from 'path';

const WASM_PATH = resolve('./src/wasm/index_bg.wasm');
const METADATA_PATH = resolve('./src/wasm/memory-init-metadata.js');
const METADATA_DTS_PATH = resolve('./src/wasm/memory-init-metadata.d.ts');

// WASM opcodes
const OP_I32_CONST = 0x41;
const OP_PREFIX = 0xfc;
const OP_MEMORY_INIT = 8;
const OP_MEMORY_COPY = 10;
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

function encodeU32LEB(value) {
  const bytes = [];
  do {
    let byte = value & 0x7f;
    value >>>= 7;
    if (value !== 0) byte |= 0x80;
    bytes.push(byte);
  } while (value !== 0);
  return new Uint8Array(bytes);
}

function encodeI32LEB(value) {
  const bytes = [];
  while (true) {
    let byte = value & 0x7f;
    value >>= 7;
    if ((value === 0 && (byte & 0x40) === 0) || (value === -1 && (byte & 0x40) !== 0)) {
      bytes.push(byte);
      break;
    }
    bytes.push(byte | 0x80);
  }
  return new Uint8Array(bytes);
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
      // function import: typeidx
      [, pos] = readU32LEB(buf, pos);
      funcImports++;
    } else if (kind === 1) {
      // table import: reftype + limits
      pos++; // reftype
      let flags; [flags, pos] = readU32LEB(buf, pos);
      [, pos] = readU32LEB(buf, pos); // min
      if (flags & 1) [, pos] = readU32LEB(buf, pos); // max
    } else if (kind === 2) {
      // memory import: limits (may have shared flag)
      let flags; [flags, pos] = readU32LEB(buf, pos);
      [, pos] = readU32LEB(buf, pos); // min
      if (flags & 1) [, pos] = readU32LEB(buf, pos); // max
    } else if (kind === 3) {
      // global import: valtype + mut
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

// Returns { bodySizePos, bodyStart, bodySize } for a function in the code section
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

// ---- Phase 1: Analyze __wasm_init_memory (extract metadata, no patching) ----

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
      // The very first i32.const in the function is the flag address
      // (used by the atomic cmpxchg at the top of the function)
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
      // Skip other prefix opcodes
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

// ---- Phase 2: Patch __wasm_init_tls (memory.init → memory.copy) ----

function patchInitTls(buf, bodyStart, bodySize, tdataDestOffset) {
  const bodyEnd = bodyStart + bodySize;

  let pos = bodyStart;
  let localCount;
  [localCount, pos] = readU32LEB(buf, pos);
  for (let i = 0; i < localCount; i++) { [, pos] = readU32LEB(buf, pos); pos++; }

  while (pos < bodyEnd) {
    const op = buf[pos];
    if (op === OP_PREFIX) {
      const prefixPos = pos;
      pos++;
      let subOp;
      [subOp, pos] = readU32LEB(buf, pos);
      if (subOp === OP_MEMORY_INIT) {
        let segIdx;
        [segIdx, pos] = readU32LEB(buf, pos);
        [, pos] = readU32LEB(buf, pos); // memIdx
        const memInitEnd = pos;

        // Scan from code start to find i32.const positions before memory.init
        let scanPos = bodyStart;
        [localCount, scanPos] = readU32LEB(buf, scanPos);
        for (let i = 0; i < localCount; i++) { [, scanPos] = readU32LEB(buf, scanPos); scanPos++; }

        const constPositions = [];
        while (scanPos < prefixPos) {
          if (buf[scanPos] === OP_I32_CONST) {
            const cPos = scanPos;
            scanPos++;
            let val;
            [val, scanPos] = readI32LEB(buf, scanPos);
            constPositions.push({ pos: cPos, endPos: scanPos, value: val });
          } else {
            scanPos++;
            switch (buf[scanPos - 1]) {
              case 0x20: case 0x21: case 0x22: case 0x23: case 0x24:
              case 0x0c: case 0x0d: case 0x10:
                [, scanPos] = readU32LEB(buf, scanPos); break;
              case 0x41: [, scanPos] = readI32LEB(buf, scanPos); break;
              case 0x02: case 0x03: case 0x04: scanPos++; break;
            }
          }
        }

        if (constPositions.length < 2) {
          throw new Error('Could not find i32.const operands before memory.init in __wasm_init_tls');
        }
        const srcConst = constPositions[constPositions.length - 2];
        const lenConst = constPositions[constPositions.length - 1];

        console.log(`  Found memory.init $.tdata in __wasm_init_tls:`);
        console.log(`    src_offset: i32.const ${srcConst.value} at [${srcConst.pos}..${srcConst.endPos}]`);
        console.log(`    length:     i32.const ${lenConst.value} at [${lenConst.pos}..${lenConst.endPos}]`);
        console.log(`    memory.init at [${prefixPos}..${memInitEnd}]`);

        // Build replacement: i32.const <tdataDestOffset> ; i32.const <len> ; memory.copy 0 0
        const newSrcConst = new Uint8Array([OP_I32_CONST, ...encodeI32LEB(tdataDestOffset)]);
        const oldLenBytes = buf.slice(lenConst.pos, lenConst.endPos);
        const newMemCopy = new Uint8Array([OP_PREFIX, ...encodeU32LEB(OP_MEMORY_COPY), 0x00, 0x00]);

        const oldSequenceStart = srcConst.pos;
        const oldSequenceEnd = memInitEnd;
        const newSequence = new Uint8Array(newSrcConst.length + oldLenBytes.length + newMemCopy.length);
        newSequence.set(newSrcConst, 0);
        newSequence.set(oldLenBytes, newSrcConst.length);
        newSequence.set(newMemCopy, newSrcConst.length + oldLenBytes.length);

        const sizeDiff = newSequence.length - (oldSequenceEnd - oldSequenceStart);
        console.log(`    Replacement: ${oldSequenceEnd - oldSequenceStart} bytes → ${newSequence.length} bytes (${sizeDiff >= 0 ? '+' : ''}${sizeDiff})`);

        return {
          spliceStart: oldSequenceStart,
          spliceEnd: oldSequenceEnd,
          newBytes: newSequence,
          sizeDiff,
        };
      }
      continue;
    }
    pos++;
    switch (buf[pos - 1]) {
      case 0x20: case 0x21: case 0x22: case 0x23: case 0x24:
      case 0x0c: case 0x0d: case 0x10:
        [, pos] = readU32LEB(buf, pos); break;
      case 0x41: [, pos] = readI32LEB(buf, pos); break;
      case 0x02: case 0x03: case 0x04: pos++; break;
      case 0x42: while (buf[pos++] & 0x80); break;
      case 0x43: pos += 4; break;
      case 0x44: pos += 8; break;
      case 0x28: case 0x29: case 0x2a: case 0x2b: case 0x2c: case 0x2d:
      case 0x2e: case 0x2f: case 0x30: case 0x31: case 0x32: case 0x33:
      case 0x34: case 0x35: case 0x36: case 0x37: case 0x38: case 0x39:
      case 0x3a: case 0x3b: case 0x3c: case 0x3d: case 0x3e:
        [, pos] = readU32LEB(buf, pos); [, pos] = readU32LEB(buf, pos); break;
      case 0x3f: case 0x40:
        [, pos] = readU32LEB(buf, pos); break;
      case 0xfe: {
        let aop; [aop, pos] = readU32LEB(buf, pos);
        if (aop <= 0x3e) { [, pos] = readU32LEB(buf, pos); [, pos] = readU32LEB(buf, pos); }
        break;
      }
    }
  }
  throw new Error('memory.init not found in __wasm_init_tls');
}

function rebuildBinaryWithSplice(buf, sections, codeSection, bodySizePos, oldBodySize, spliceStart, spliceEnd, newBytes, sizeDiff) {
  const oldBodySizeLEB = encodeU32LEB(oldBodySize);
  const newBodySizeLEB = encodeU32LEB(oldBodySize + sizeDiff);

  const oldSectionSizeLEB = encodeU32LEB(codeSection.size);
  const newSectionSizeLEB = encodeU32LEB(codeSection.size + sizeDiff + (newBodySizeLEB.length - oldBodySizeLEB.length));

  const totalSizeDiff = sizeDiff
    + (newBodySizeLEB.length - oldBodySizeLEB.length)
    + (newSectionSizeLEB.length - oldSectionSizeLEB.length);

  const newBuf = new Uint8Array(buf.length + totalSizeDiff);
  let writePos = 0;

  newBuf.set(buf.slice(0, codeSection.sizePos), writePos);
  writePos += codeSection.sizePos;

  newBuf.set(newSectionSizeLEB, writePos);
  writePos += newSectionSizeLEB.length;

  const afterOldSectionSize = codeSection.sizePos + oldSectionSizeLEB.length;
  newBuf.set(buf.slice(afterOldSectionSize, bodySizePos), writePos);
  writePos += bodySizePos - afterOldSectionSize;

  newBuf.set(newBodySizeLEB, writePos);
  writePos += newBodySizeLEB.length;

  const afterOldBodySize = bodySizePos + oldBodySizeLEB.length;
  newBuf.set(buf.slice(afterOldBodySize, spliceStart), writePos);
  writePos += spliceStart - afterOldBodySize;

  newBuf.set(newBytes, writePos);
  writePos += newBytes.length;

  newBuf.set(buf.slice(spliceEnd), writePos);
  writePos += buf.length - spliceEnd;

  if (writePos !== newBuf.length) {
    throw new Error(`Size mismatch: wrote ${writePos}, expected ${newBuf.length}`);
  }

  return newBuf;
}

// ---- Main ----

let buf = new Uint8Array(readFileSync(WASM_PATH));
console.log(`Reading ${WASM_PATH} (${buf.length} bytes)`);

let sections = findSections(buf);
console.log(`Found ${sections.length} sections`);

const importCount = countImports(buf, sections);
console.log(`Import count: ${importCount} functions`);

// ---- Phase 1: Analyze __wasm_init_memory (extract metadata only) ----

console.log('\n--- Phase 1: Analyze __wasm_init_memory ---');
const initMemIdx = findFunctionByName(buf, sections, '__wasm_init_memory');
if (initMemIdx < 0) { console.error('ERROR: __wasm_init_memory not found'); process.exit(1); }
console.log(`Found __wasm_init_memory at function index ${initMemIdx}`);

const initMemBody = locateFunctionBody(buf, sections, initMemIdx, importCount);
console.log(`Function body: offset=${initMemBody.bodyStart}, size=${initMemBody.bodySize}`);

const { segments, bss, flagAddress } = analyzeInitMemory(buf, initMemBody.bodyStart, initMemBody.bodySize);
if (segments.length === 0) { console.error('ERROR: No memory.init found in __wasm_init_memory'); process.exit(1); }
if (flagAddress === null) { console.error('ERROR: Could not extract flag address from __wasm_init_memory'); process.exit(1); }
console.log(`Flag address: ${flagAddress}`);

// ---- Phase 2: Patch __wasm_init_tls (memory.init → memory.copy) ----

console.log('\n--- Phase 2: Patch __wasm_init_tls ---');
const initTlsIdx = findFunctionByName(buf, sections, '__wasm_init_tls');
if (initTlsIdx < 0) { console.error('ERROR: __wasm_init_tls not found'); process.exit(1); }
console.log(`Found __wasm_init_tls at function index ${initTlsIdx}`);

const initTlsBody = locateFunctionBody(buf, sections, initTlsIdx, importCount);
console.log(`Function body: offset=${initTlsBody.bodyStart}, size=${initTlsBody.bodySize}`);

const tdataSegment = segments.find(s => s.segIdx === 0);
if (!tdataSegment) { console.error('ERROR: .tdata segment (idx 0) not found'); process.exit(1); }
const tdataDestOffset = tdataSegment.destOffset;
console.log(`TLS template address: ${tdataDestOffset}`);

const tlsPatch = patchInitTls(buf, initTlsBody.bodyStart, initTlsBody.bodySize, tdataDestOffset);

const codeSection = sections.find(s => s.id === SECTION_CODE);
buf = rebuildBinaryWithSplice(
  buf, sections, codeSection,
  initTlsBody.bodySizePos, initTlsBody.bodySize,
  tlsPatch.spliceStart, tlsPatch.spliceEnd, tlsPatch.newBytes, tlsPatch.sizeDiff
);
console.log(`Rebuilt binary: ${buf.length} bytes`);

// ---- Write outputs ----

writeFileSync(WASM_PATH, buf);
console.log(`\nWrote patched WASM to ${WASM_PATH}`);

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
console.log(`\n✅ Safari memory init patch complete (Phase 2 only — __wasm_init_tls)`);
