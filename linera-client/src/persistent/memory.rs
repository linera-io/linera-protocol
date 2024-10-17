// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{Dirty, Persist};

pub type Error = std::convert::Infallible;

/// A dummy [`Persist`] implementation that doesn't persist anything, but holds the value
/// in memory.
#[derive(derive_more::Deref)]
pub struct Memory<T> {
    #[deref]
    value: T,
    dirty: Dirty,
}

impl<T> Memory<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            dirty: Dirty::new(true),
        }
    }
}

impl<T: Send> Persist for Memory<T> {
    type Error = Error;

    fn as_mut(&mut self) -> &mut T {
        *self.dirty = true;
        &mut self.value
    }

    fn into_value(self) -> T {
        self.value
    }

    async fn persist(&mut self) -> Result<(), Error> {
        *self.dirty = false;
        Ok(())
    }
}
