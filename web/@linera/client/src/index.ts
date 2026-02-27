export * from './wasm/index.js';
export * from './error/index.js';
export * as signer from './signer/index.js';
export type { Signer } from './signer/Signer.js';

import * as wasm from './wasm/index.js';

import Bowser from 'bowser';

function isBrokenSafari(): boolean {
  const browser = Bowser.getParser(window.navigator.userAgent);
  return browser.satisfies({
    safari: "~26.2"
  }) || false;
}

export async function initialize(options?: wasm.InitializeOptions) {
  if (window.location) {
    // Allow overriding the application's log filters using the `LINERA_LOG` and
    // `LINERA_PROFILING` search params, for debugging.
    const params = new URL(window.location.href).searchParams;
    const log = params.get('LINERA_LOG');

    if (!options) options = {};
    if (params.get('LINERA_PROFILING')) options.profiling = true;
    if (log) options.log = log;
  }

  const exports = await wasm.default();

  // Safari 26.2 crashes with shared WebAssembly memory during
  // multi-threaded operation (WebKit #303387). Rust's wasm32 allocator
  // (dlmalloc) uses via memory.grow to decide how much memory is left
  // so starting with INITIAL and MAXIMUM values that are too close
  // to the actual memory limit causes it to grow memory and crash.
  // Pre-allocating a large block while still single-threaded prevents the crash —
  // avoids memory.grow calls once worker threads are running.
  if (isBrokenSafari()) {
    const PREALLOC_BYTES = 768 * 1024 * 1024;
    try {
      const ptr = exports.__wbindgen_malloc(PREALLOC_BYTES, 1);
      if (ptr !== 0) {
        exports.__wbindgen_free(ptr, PREALLOC_BYTES, 1);
      }
    } catch {
      // Pre-allocation is best-effort
    }
  }

  wasm.initialize(options);
}
