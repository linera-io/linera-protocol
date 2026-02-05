export * from './wasm/index.js';
export * as signer from './signer/index.js';
export type { Signer } from './signer/Signer.js';

import * as wasm from './wasm/index.js';
import { initializeSharedMemory } from './wasm-memory-init.js';

function diagLog(msg: string) {
  console.log(`[linera-init] ${msg}`);
  try {
    const prev = sessionStorage.getItem('linera_diag') || '';
    sessionStorage.setItem('linera_diag', prev + `[${Date.now()}] ${msg}\n`);
  } catch {}
}

export async function initialize(options?: wasm.InitializeOptions) {
  // Show any logs from a previous session (survives renderer crashes)
  try {
    const prev = sessionStorage.getItem('linera_diag');
    if (prev) {
      console.warn('[linera-init] Previous session logs:\n' + prev);
      sessionStorage.removeItem('linera_diag');
    }
  } catch {}

  if (window.location) {
    const params = new URL(window.location.href).searchParams;
    const defaults: wasm.InitializeOptions = {};
    defaults.profiling = params.get('LINERA_PROFILING') !== null;
    defaults.log = params.get('LINERA_LOG') || '';
    options = { ...defaults, ...options };
  }

  diagLog('Fetching WASM binary...');
  const wasmUrl = new URL('./wasm/index_bg.wasm', import.meta.url);
  const resp = await fetch(wasmUrl.href);
  const wasmBytes = await resp.arrayBuffer();
  diagLog(`Fetched ${wasmBytes.byteLength} bytes`);

  // Safari 26-26.2 has a WebKit bug where memory.grow on shared memory
  // crashes the renderer during multi-threaded operation (WebKit #304386).
  // Rust's wasm32 allocator (dlmalloc) gets memory exclusively through
  // memory.grow, so we cannot use fixed-size memory (initial == maximum).
  // Instead, we use a moderate initial allocation and then force dlmalloc
  // to pre-grow while still single-threaded (see below).
  const INITIAL_PAGES = 512; // 32MB — enough for data segments + initial allocs
  const MAX_PAGES = 16384;   // 1GB

  diagLog(`Creating WebAssembly.Memory (initial: ${INITIAL_PAGES}, max: ${MAX_PAGES}, shared: true)`);
  const memory = new WebAssembly.Memory({
    initial: INITIAL_PAGES,
    maximum: MAX_PAGES,
    shared: true,
  });
  diagLog(`Memory created, buffer size: ${memory.buffer.byteLength}`);

  diagLog('Initializing shared memory segments from JS...');
  initializeSharedMemory(memory, wasmBytes);
  diagLog('Shared memory initialized');

  diagLog('Instantiating WASM module...');
  const exports = await wasm.default({ module_or_path: wasmBytes, memory });
  diagLog('WASM instantiated');

  // Force dlmalloc to pre-allocate a large memory pool via memory.grow
  // while still single-threaded. This way, multi-threaded code can allocate
  // from dlmalloc's existing pool without triggering memory.grow, avoiding
  // the Safari 26-26.2 crash.
  const PREALLOC_BYTES = 768 * 1024 * 1024; // 768MB
  diagLog(`Pre-allocating ${PREALLOC_BYTES / 1024 / 1024}MB to prime allocator...`);
  try {
    const wasmExports = exports as any;
    const ptr = wasmExports.__wbindgen_malloc(PREALLOC_BYTES, 1);
    if (ptr !== 0) {
      wasmExports.__wbindgen_free(ptr, PREALLOC_BYTES, 1);
      diagLog(`Pre-allocation done (memory: ${memory.buffer.byteLength / 1024 / 1024}MB)`);
    } else {
      diagLog('Pre-allocation returned null pointer');
    }
  } catch (e: any) {
    diagLog(`Pre-allocation failed: ${e.message}`);
  }

  diagLog('Calling wasm.initialize()...');
  wasm.initialize(options);
  diagLog('Initialization complete');
}
