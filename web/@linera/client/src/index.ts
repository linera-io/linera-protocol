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

  // Safari 26-26.2 has a WebKit bug where the per-thread memory size cache
  // isn't updated across threads after memory.grow (WebKit commit 299880).
  // Rust's wasm32 allocator (dlmalloc) gets all memory via memory.grow, so
  // normal operation triggers cross-thread cache staleness → crashes.
  //
  // Workaround: pre-allocate a large block while still single-threaded to
  // fill dlmalloc's pool. After this, allocations come from the pool without
  // triggering memory.grow, so the stale cache bug is never hit.
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
