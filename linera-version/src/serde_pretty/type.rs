// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// A wrapper that serializes a value of type `T` through a more human-readable
/// representation type `Repr`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Pretty<T, Repr> {
    /// The wrapped value.
    pub value: T,
    _phantom: std::marker::PhantomData<fn(Repr) -> Repr>,
}

impl<T, Repr> Pretty<T, Repr> {
    /// Wraps the given value.
    pub const fn new(value: T) -> Self {
        Pretty {
            value,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Converts the wrapped value into its representation type.
    pub fn repr(self) -> Repr
    where
        Repr: From<T>,
    {
        Repr::from(self.value)
    }
}

impl<T, Repr> std::fmt::Display for Pretty<T, Repr>
where
    T: Clone,
    Repr: std::fmt::Display + From<T>,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "{}", Repr::from(self.value.clone()))
    }
}
