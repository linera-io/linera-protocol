// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A cache of compiled WebAssembly modules.
//!
//! The cache is limited by the total size of cached bytecodes. Note that this is a heuristic to
//! estimate the total memory usage by the cache, since it's currently not possible to determine
//! the size of a generic `Module`.

use crate::{wasm::WasmExecutionError, Bytecode};
use lru::LruCache;
use std::sync::Arc;

/// The default maximum size of the bytecodes stored in cache.
const DEFAULT_MAX_CACHE_SIZE: u64 = 512 /* MiB */ * 1024 /* KiB */ * 1024 /* bytes */;

/// A cache of compiled WebAssembly modules.
///
/// The cache prioritizes entries based on their [`Metadata`].
pub struct ModuleCache<Module> {
    modules: LruCache<Bytecode, Arc<Module>>,
    total_size: u64,
    max_size: u64,
}

impl<Module> Default for ModuleCache<Module> {
    fn default() -> Self {
        ModuleCache {
            modules: LruCache::unbounded(),
            total_size: 0,
            max_size: DEFAULT_MAX_CACHE_SIZE,
        }
    }
}

impl<Module> ModuleCache<Module> {
    /// Returns a `Module` for the requested `bytecode`, creating it with `module_builder` and
    /// adding it to the cache if it doesn't already exist in the cache.
    pub fn get_or_insert_with<E>(
        &mut self,
        bytecode: Bytecode,
        module_builder: impl FnOnce(Bytecode) -> Result<Module, E>,
    ) -> Result<Arc<Module>, WasmExecutionError>
    where
        WasmExecutionError: From<E>,
    {
        if let Some(module) = self.get(&bytecode) {
            Ok(module)
        } else {
            let module = Arc::new(module_builder(bytecode.clone())?);
            self.insert(bytecode, module.clone());
            Ok(module)
        }
    }

    /// Returns a `Module` for the requested `bytecode` if it's in the cache.
    pub fn get(&mut self, bytecode: &Bytecode) -> Option<Arc<Module>> {
        self.modules.get(bytecode).cloned()
    }

    /// Inserts a `bytecode` and its compiled `module` in the cache.
    pub fn insert(&mut self, bytecode: Bytecode, module: Arc<Module>) {
        let bytecode_size = bytecode.as_ref().len() as u64;

        if self.total_size + bytecode_size > self.max_size {
            self.reduce_size_to(self.max_size - bytecode_size);
        }

        self.modules.put(bytecode, module);
    }

    /// Evicts entries from the cache so that the total size of cached bytecodes is less than
    /// `new_size`.
    fn reduce_size_to(&mut self, new_size: u64) {
        while self.total_size > new_size {
            let (bytecode, _module) = self
                .modules
                .pop_lru()
                .expect("Empty cache should have a `total_size` of zero");
            let bytecode_size = bytecode.as_ref().len() as u64;

            self.total_size -= bytecode_size;
        }
    }
}
