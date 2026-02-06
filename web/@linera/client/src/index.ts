export * from './wasm/index.js';
export * as signer from './signer/index.js';
export type { Signer } from './signer/Signer.js';

import * as wasm from './wasm/index.js';

function isSafari(): boolean {
  try {
    const ua = navigator.userAgent;
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

  const exports = await wasm.default();

  // Safari 26-26.2 crashes with shared WebAssembly memory during
  // multi-threaded operation (WebKit #303387). Pre-allocating a large block
  // while still single-threaded prevents the crash — Rust's wasm32 allocator
  // (dlmalloc) acquires all memory via memory.grow, so filling its pool here
  // avoids memory.grow calls once worker threads are running.
  if (isSafari()) {
    const PREALLOC_BYTES = 768 * 1024 * 1024;
    try {
      const ex = exports as any;
      const ptr = ex.__wbindgen_malloc(PREALLOC_BYTES, 1);
      if (ptr !== 0) {
        ex.__wbindgen_free(ptr, PREALLOC_BYTES, 1);
      }
    } catch {
      // Pre-allocation is best-effort
    }
  }

  wasm.initialize(options);
}
