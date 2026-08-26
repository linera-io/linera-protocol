// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An in-memory [`Persist`] backend that holds the value without writing it anywhere.

use super::Persist;

/// The error type of the in-memory backend: persisting can never fail.
pub type Error = std::convert::Infallible;

/// A dummy [`Persist`] implementation that doesn't persist anything, but holds the value
/// in memory.
#[derive(Default, derive_more::Deref)]
pub struct Memory<T> {
    #[deref]
    value: T,
}

impl<T> Memory<T> {
    /// Wraps a value in an in-memory backend.
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

impl<T: Send> Persist for Memory<T> {
    type Error = Error;

    fn as_mut(&mut self) -> &mut T {
        &mut self.value
    }

    fn into_value(self) -> T {
        self.value
    }

    async fn persist(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
