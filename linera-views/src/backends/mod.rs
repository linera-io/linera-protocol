// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod journaling;

#[cfg(with_metrics)]
pub mod metering;

pub mod value_splitting;

pub mod memory;

pub mod lru_caching;

pub mod dual;

#[cfg(with_scylladb)]
pub mod scylla_db;

#[cfg(with_rocksdb)]
pub mod rocks_db;

#[cfg(with_indexeddb)]
pub mod indexed_db;

#[cfg(with_testing)]
/// Serializes the contents of a database namespace to disk for test backup/restore.
pub trait DatabaseBackup {
    /// Writes a snapshot of the namespace into `dir`.
    fn backup_to(&self, dir: &std::path::Path) -> anyhow::Result<()>;
}
