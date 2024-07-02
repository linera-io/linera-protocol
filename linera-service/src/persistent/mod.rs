// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod file;

use std::ops::{Deref, DerefMut};

pub use file::File;

pub trait Persist: Deref {
    type Error: std::fmt::Debug;

    fn as_mut(_: &mut Self) -> &mut Self::Target;
    fn persist(_: &mut Self) -> Result<(), Self::Error>;
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
