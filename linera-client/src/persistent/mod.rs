// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod dirty;
use dirty::Dirty;

cfg_if::cfg_if! {
    if #[cfg(with_indexed_db)] {
        pub mod indexed_db;
        pub use indexed_db::IndexedDb;
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "fs")] {
        pub mod file;
        pub use file::File;
    }
}

pub mod memory;
use std::ops::Deref;

pub use memory::Memory;

/// The `Persist` trait provides a wrapper around a value that can be saved in a
/// persistent way. A minimal implementation provides an `Error` type, a `persist`
/// function to persist the value, and an `as_mut` function to get a mutable reference to
/// the value in memory.
///
/// `LocalPersist` is a non-`Send` version.
#[trait_variant::make(Persist: Send)]
pub trait LocalPersist: Deref {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Get a mutable reference to the value.  This is not expressed as a
    /// [`DerefMut`](std::ops::DerefMut) bound because it is discouraged to consume this
    /// function!  Instead, use `mutate`.
    fn as_mut(&mut self) -> &mut Self::Target;

    /// Saves the value to persistent storage.
    async fn persist(&mut self) -> Result<(), Self::Error>;

    /// Takes the value out.
    fn into_value(self) -> Self::Target
    where
        Self::Target: Sized;
}

#[allow(async_fn_in_trait)]
pub trait LocalPersistExt: LocalPersist {
    /// Applies a mutation to the value, persisting when done.
    async fn mutate<R>(
        &mut self,
        mutation: impl FnOnce(&mut Self::Target) -> R,
    ) -> Result<R, Self::Error>;
}

#[trait_variant::make(Send)]
pub trait PersistExt: Persist {
    /// Applies a mutation to the value, persisting when done.
    async fn mutate<R: Send>(
        &mut self,
        mutation: impl FnOnce(&mut Self::Target) -> R + Send,
    ) -> Result<R, Self::Error>;
}

#[allow(async_fn_in_trait)]
impl<T: LocalPersist> LocalPersistExt for T {
    async fn mutate<R>(
        &mut self,
        mutation: impl FnOnce(&mut Self::Target) -> R,
    ) -> Result<R, Self::Error> {
        let output = mutation(self.as_mut());
        self.persist().await?;
        Ok(output)
    }
}

impl<T: Persist> PersistExt for T {
    async fn mutate<R>(
        &mut self,
        mutation: impl FnOnce(&mut Self::Target) -> R + Send,
    ) -> Result<R, Self::Error> {
        let output = mutation(self.as_mut());
        self.persist().await?;
        Ok(output)
    }
}
