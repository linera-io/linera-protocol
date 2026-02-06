export * from './wasm/index.js';
export * as signer from './signer/index.js';
export type { Signer } from './signer/Signer.js';

import * as wasm from './wasm/index.js';
import { MEMORY_INIT_METADATA } from './wasm/memory-init-metadata.js';
import { initializeSharedMemory } from './wasm-memory-init.js';

function isSafari(): boolean {
  try {
    const ua = navigator.userAgent;
    // Safari: contains "Safari/" but not "Chrome/" or "Chromium/"
    return /Safari\//.test(ua) && !/Chrome\/|Chromium\//.test(ua);
  } catch {
    return false;
  }
}

export async function initialize(options?: wasm.InitializeOptions) {
  if (window.location) {
    const params = new URL(window.location.href).searchParams;
    const defaults: wasm.InitializeOptions = {};
    defaults.profiling = params.get('LINERA_PROFILING') !== null;
    defaults.log = params.get('LINERA_LOG') || '';
    options = { ...defaults, ...options };
  }

  if (isSafari()) {
    await initializeSafari(options);
  } else {
    await wasm.default();
    wasm.initialize(options);
  }
}

// Safari 26-26.2 has two WebKit bugs with shared WebAssembly memory:
//   1. `memory.init` on shared memory traps (crashes the module)
//   2. `memory.grow` during multi-threaded operation crashes the renderer
//      (WebKit #304386)
//
// Workaround:
//   - Create Memory from JS, copy data segments before instantiation
//   - Pre-set __wasm_init_memory's atomic flag to 2 so it skips memory.init
//   - __wasm_init_tls is patched at build time (memory.init → memory.copy)
//   - Pre-allocate 768MB via malloc/free while single-threaded to fill
//     dlmalloc's pool, avoiding memory.grow during multi-threaded operation
async function initializeSafari(options?: wasm.InitializeOptions) {
  const wasmUrl = new URL('./wasm/index_bg.wasm', import.meta.url);
  const wasmBytes = await fetch(wasmUrl.href).then(r => r.arrayBuffer());

  const INITIAL_PAGES = 512;  // 32MB
  const MAX_PAGES = 16384;    // 1GB
  const memory = new WebAssembly.Memory({
    initial: INITIAL_PAGES,
    maximum: MAX_PAGES,
    shared: true,
  });

  // Copy data segments into shared memory before instantiation
  initializeSharedMemory(memory, wasmBytes);

  // Pre-set __wasm_init_memory's atomic flag to 2 ("already initialized").
  // This makes the function skip all memory.init calls (which would crash).
  const flagIndex = MEMORY_INIT_METADATA.flagAddress / 4;
  Atomics.store(new Int32Array(memory.buffer), flagIndex, 2);

  const exports = await wasm.default({ module_or_path: wasmBytes, memory });

  // Pre-allocate a large memory pool while still single-threaded.
  // Rust's wasm32 allocator (dlmalloc) gets all memory via memory.grow.
  // By allocating and freeing a large block now, dlmalloc's internal pool
  // is primed so multi-threaded code can allocate without memory.grow.
  const PREALLOC_BYTES = 768 * 1024 * 1024;
  try {
    const ex = exports as any;
    const ptr = ex.__wbindgen_malloc(PREALLOC_BYTES, 1);
    if (ptr !== 0) {
      ex.__wbindgen_free(ptr, PREALLOC_BYTES, 1);
    }
  } catch {
    // Pre-allocation is best-effort; failure here is non-fatal
  }

  wasm.initialize(options);
}
