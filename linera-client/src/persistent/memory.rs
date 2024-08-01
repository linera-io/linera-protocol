// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::Deref;

use super::Persist;

pub type Error = std::convert::Infallible;

/// A dummy [`Persist`] implementation that doesn't persist anything, but holds the value
/// in memory.
pub struct Memory<T>(T);

impl<T> Deref for Memory<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> Memory<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T> Persist for Memory<T> {
    type Error = Error;

    fn as_mut(this: &mut Self) -> &mut T {
        &mut this.0
    }

    fn into_value(this: Self) -> T {
        this.0
    }

    fn persist(_this: &mut Self) -> Result<(), Error> {
        Ok(())
    }
}
