// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod balance_scalar;

use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use thiserror::Error;

#[cfg(not(target_arch = "wasm32"))]
use std::fmt;
#[cfg(any(test, feature = "test"))]
use test_strategy::Arbitrary;

#[cfg(not(target_arch = "wasm32"))]
use chrono::NaiveDateTime;

use crate::doc_scalar;

/// A non-negative amount of money to be transferred.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct Amount(u64);

/// The balance of a chain.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct Balance(u128);

/// A block height to identify blocks in a chain.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
pub struct BlockHeight(pub u64);

/// A number to identify successive attempts to decide a value in a consensus protocol.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct RoundNumber(pub u64);

/// A timestamp, in microseconds since the Unix epoch.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Returns the current time according to the system clock.
    pub fn now() -> Timestamp {
        Timestamp(
            SystemTime::UNIX_EPOCH
                .elapsed()
                .expect("system time should be after Unix epoch")
                .as_micros()
                .try_into()
                .unwrap_or(u64::MAX),
        )
    }

    /// Returns the number of microseconds since the Unix epoch.
    pub fn micros(&self) -> u64 {
        self.0
    }

    /// Returns the number of microseconds from `other` until `self`, or `0` if `other` is not
    /// earlier than `self`.
    pub fn saturating_diff_micros(&self, other: Timestamp) -> u64 {
        self.0.saturating_sub(other.0)
    }
}

impl From<u64> for Timestamp {
    fn from(t: u64) -> Timestamp {
        Timestamp(t)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(date_time) = NaiveDateTime::from_timestamp_opt(
            (self.0 / 1_000_000) as i64,
            ((self.0 % 1_000_000) * 1_000) as u32,
        ) {
            return date_time.fmt(f);
        }
        self.0.fmt(f)
    }
}

#[derive(Debug, Error)]
/// An error type for arithmetic errors.
pub enum ArithmeticError {
    #[error("Number overflow")]
    Overflow,
    #[error("Number underflow")]
    Underflow,
}

macro_rules! impl_strictly_wrapped_number {
    ($name:ident, $wrapped:ident) => {
        impl $name {
            pub fn zero() -> Self {
                Self(0)
            }

            pub fn max() -> Self {
                Self($wrapped::MAX)
            }

            pub fn try_add(self, other: Self) -> Result<Self, ArithmeticError> {
                let val = self
                    .0
                    .checked_add(other.0)
                    .ok_or(ArithmeticError::Overflow)?;
                Ok(Self(val))
            }

            pub fn try_add_one(self) -> Result<Self, ArithmeticError> {
                let val = self.0.checked_add(1).ok_or(ArithmeticError::Overflow)?;
                Ok(Self(val))
            }

            pub fn saturating_add(self, other: Self) -> Self {
                let val = self.0.saturating_add(other.0);
                Self(val)
            }

            pub fn try_sub(self, other: Self) -> Result<Self, ArithmeticError> {
                let val = self
                    .0
                    .checked_sub(other.0)
                    .ok_or(ArithmeticError::Underflow)?;
                Ok(Self(val))
            }

            pub fn try_sub_one(self) -> Result<Self, ArithmeticError> {
                let val = self.0.checked_sub(1).ok_or(ArithmeticError::Underflow)?;
                Ok(Self(val))
            }

            pub fn saturating_sub(self, other: Self) -> Self {
                let val = self.0.saturating_sub(other.0);
                Self(val)
            }

            pub fn try_add_assign(&mut self, other: Self) -> Result<(), ArithmeticError> {
                self.0 = self
                    .0
                    .checked_add(other.0)
                    .ok_or(ArithmeticError::Overflow)?;
                Ok(())
            }

            pub fn try_add_assign_one(&mut self) -> Result<(), ArithmeticError> {
                self.0 = self.0.checked_add(1).ok_or(ArithmeticError::Overflow)?;
                Ok(())
            }

            pub fn saturating_add_assign(&mut self, other: Self) {
                self.0 = self.0.saturating_add(other.0);
            }

            pub fn try_sub_assign(&mut self, other: Self) -> Result<(), ArithmeticError> {
                self.0 = self
                    .0
                    .checked_sub(other.0)
                    .ok_or(ArithmeticError::Underflow)?;
                Ok(())
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl std::str::FromStr for $name {
            type Err = std::num::ParseIntError;

            fn from_str(src: &str) -> Result<Self, Self::Err> {
                Ok(Self($wrapped::from_str(src)?))
            }
        }

        impl From<$name> for $wrapped {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        // Cannot directly create values for a strictly wrapped type, except for testing.
        #[cfg(any(test, feature = "test"))]
        impl From<$wrapped> for $name {
            fn from(value: $wrapped) -> Self {
                Self(value)
            }
        }
    };
}

macro_rules! impl_wrapped_number {
    ($name:ident, $wrapped:ident) => {
        impl_strictly_wrapped_number!($name, $wrapped);

        #[cfg(not(any(test, feature = "test")))]
        impl From<$wrapped> for $name {
            fn from(value: $wrapped) -> Self {
                Self(value)
            }
        }

        impl TryInto<usize> for $name {
            type Error = ArithmeticError;

            fn try_into(self) -> Result<usize, ArithmeticError> {
                usize::try_from(self.0).map_err(|_| ArithmeticError::Overflow)
            }
        }

        impl TryFrom<usize> for $name {
            type Error = ArithmeticError;

            fn try_from(value: usize) -> Result<$name, ArithmeticError> {
                $wrapped::try_from(value)
                    .map_err(|_| ArithmeticError::Overflow)
                    .map(Self)
            }
        }
    };
}

impl_wrapped_number!(Balance, u128);
impl_strictly_wrapped_number!(Amount, u64);
impl_wrapped_number!(BlockHeight, u64);
impl_strictly_wrapped_number!(RoundNumber, u64);

impl<'a> std::iter::Sum<&'a Amount> for Amount {
    fn sum<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
        iter.fold(Self::zero(), |a, b| a.saturating_add(*b))
    }
}

impl Balance {
    /// Helper function to obtain the 64 most significant bits of the balance.
    pub fn upper_half(self) -> u64 {
        (self.0 >> 64) as u64
    }

    /// Helper function to obtain the 64 least significant bits of the balance.
    pub fn lower_half(self) -> u64 {
        self.0 as u64
    }
}

impl From<Amount> for Balance {
    fn from(val: Amount) -> Self {
        Balance(val.0 as u128)
    }
}

impl TryFrom<Balance> for Amount {
    type Error = std::num::TryFromIntError;

    fn try_from(val: Balance) -> Result<Self, Self::Error> {
        Ok(Amount(val.0.try_into()?))
    }
}

doc_scalar!(Amount, "A non-negative amount of money to be transferred");
doc_scalar!(BlockHeight, "A block height to identify blocks in a chain");
doc_scalar!(
    Timestamp,
    "A timestamp, in microseconds since the Unix epoch"
);
