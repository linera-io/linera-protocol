// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::Persist;

pub type Error = std::convert::Infallible;

/// A dummy [`Persist`] implementation that doesn't persist anything, but holds the value
/// in memory.
#[derive(Default, derive_more::Deref)]
pub struct Memory<T> {
    #[deref]
    value: T,
}

impl<T> Memory<T> {
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
