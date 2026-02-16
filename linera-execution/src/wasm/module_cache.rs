// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A cache of compiled WebAssembly modules.
//!
//! The cache is limited by the total size of cached bytecode files. Note that this is a heuristic to
//! estimate the total memory usage by the cache, since it's currently not possible to determine
//! the size of a generic `Module`.

use linera_base::data_types::Bytecode;
use lru::LruCache;

/// The default maximum size of the bytecode files stored in cache.
const DEFAULT_MAX_CACHE_SIZE: u64 = 512 /* MiB */ * 1024 /* KiB */ * 1024 /* bytes */;

/// A cache of compiled WebAssembly modules.
///
/// The cache prioritizes entries based on their [`Metadata`].
pub struct ModuleCache<Module> {
    modules: LruCache<Bytecode, Module>,
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
    #[cfg(test)]
    fn with_max_size(max_size: u64) -> Self {
        ModuleCache {
            modules: LruCache::unbounded(),
            total_size: 0,
            max_size,
        }
    }
}

impl<Module: Clone> ModuleCache<Module> {
    /// Returns a `Module` for the requested `bytecode`, creating it with `module_builder` and
    /// adding it to the cache if it doesn't already exist in the cache.
    pub fn get_or_insert_with<E>(
        &mut self,
        bytecode: Bytecode,
        module_builder: impl FnOnce(Bytecode) -> Result<Module, E>,
    ) -> Result<Module, E> {
        if let Some(module) = self.get(&bytecode) {
            Ok(module)
        } else {
            let module = module_builder(bytecode.clone())?;
            self.insert(bytecode, module.clone());
            Ok(module)
        }
    }

    /// Returns a `Module` for the requested `bytecode` if it's in the cache.
    pub fn get(&mut self, bytecode: &Bytecode) -> Option<Module> {
        self.modules.get(bytecode).cloned()
    }

    /// Inserts a `bytecode` and its compiled `module` in the cache.
    pub fn insert(&mut self, bytecode: Bytecode, module: Module) {
        let bytecode_size = bytecode.as_ref().len() as u64;

        if bytecode_size > self.max_size {
            return;
        }

        if self.modules.promote(&bytecode) {
            return;
        }

        if self.total_size + bytecode_size > self.max_size {
            self.reduce_size_to(self.max_size - bytecode_size);
        }

        self.modules.put(bytecode, module);
        self.total_size += bytecode_size;
    }

    /// Evicts entries from the cache so that the total size of cached bytecode files is less than
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bytecode(size: usize) -> Bytecode {
        Bytecode::new(vec![0u8; size])
    }

    fn distinct_bytecode(size: usize, discriminant: u8) -> Bytecode {
        let mut bytes = vec![0u8; size];
        bytes[0] = discriminant;
        Bytecode::new(bytes)
    }

    #[test]
    fn total_size_tracks_insertions() {
        let mut cache = ModuleCache::<u32>::with_max_size(1000);
        cache.insert(bytecode(100), 1);
        assert_eq!(cache.total_size, 100);
        cache.insert(distinct_bytecode(200, 1), 2);
        assert_eq!(cache.total_size, 300);
    }

    #[test]
    fn eviction_triggers_when_full() {
        let mut cache = ModuleCache::<u32>::with_max_size(250);
        cache.insert(bytecode(100), 1);
        cache.insert(distinct_bytecode(100, 1), 2);
        assert_eq!(cache.total_size, 200);
        assert_eq!(cache.modules.len(), 2);

        cache.insert(distinct_bytecode(100, 2), 3);
        assert_eq!(cache.modules.len(), 2);
        assert!(cache.total_size <= 250);
    }

    #[test]
    fn oversized_bytecode_is_rejected() {
        let mut cache = ModuleCache::<u32>::with_max_size(50);
        cache.insert(bytecode(100), 1);
        assert_eq!(cache.total_size, 0);
        assert_eq!(cache.modules.len(), 0);
    }

    #[test]
    fn reinserting_same_key_does_not_double_count() {
        let mut cache = ModuleCache::<u32>::with_max_size(1000);
        let bc = bytecode(100);
        cache.insert(bc.clone(), 1);
        assert_eq!(cache.total_size, 100);
        cache.insert(bc, 2);
        assert_eq!(cache.total_size, 100);
        assert_eq!(cache.modules.len(), 1);
    }

    #[test]
    fn reinserting_existing_key_does_not_evict() {
        let mut cache = ModuleCache::<u32>::with_max_size(200);
        cache.insert(bytecode(100), 1);
        cache.insert(distinct_bytecode(100, 1), 2);
        assert_eq!(cache.modules.len(), 2);

        cache.insert(bytecode(100), 3);
        assert_eq!(cache.modules.len(), 2);
        assert_eq!(cache.total_size, 200);
    }
}
