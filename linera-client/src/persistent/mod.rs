// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

cfg_if::cfg_if! {
    if #[cfg(feature = "local-storage")] {
        pub mod local_storage;
        pub use local_storage::LocalStorage;
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "fs")] {
        pub mod file;
        pub use file::File;
    }
}

pub mod memory;
use std::ops::{Deref, DerefMut};

pub use memory::Memory;

/// The `Persist` trait provides a wrapper around a value that can be saved in a
/// persistent way. A minimal implementation provides an `Error` type, a `persist`
/// function to persist the value, and an `as_mut` function to get a mutable reference to
/// the value in memory.
pub trait Persist: Deref {
    type Error: std::fmt::Debug;

    /// Gets a mutable reference to the value.
    fn as_mut(_: &mut Self) -> &mut Self::Target;

    /// Saves the value to persistent storage.
    fn persist(_: &mut Self) -> Result<(), Self::Error>;

    /// Takes the value out.
    fn into_value(this: Self) -> Self::Target;

    /// Gets a mutable reference to the value which, on drop, will automatically persist
    /// the new value.
    fn mutate(this: &mut Self) -> RefMut<Self> {
        RefMut(this)
    }
}

pub struct RefMut<'a, P: Persist + ?Sized>(&'a mut P);

impl<P: Persist> Deref for RefMut<'_, P> {
    type Target = P::Target;
    fn deref(&self) -> &P::Target {
        self.0.deref()
    }
}

impl<P: Persist> DerefMut for RefMut<'_, P> {
    fn deref_mut(&mut self) -> &mut P::Target {
        Persist::as_mut(self.0)
    }
}

impl<P: Persist + ?Sized> Drop for RefMut<'_, P> {
    fn drop(&mut self) {
        if let Err(e) = Persist::persist(self.0) {
            tracing::warn!("failed to persist value: {e:#?}");
        }
    }
}
