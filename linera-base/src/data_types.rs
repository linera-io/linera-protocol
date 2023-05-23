// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use std::{fmt, time::SystemTime};
use thiserror::Error;

#[cfg(any(test, feature = "test"))]
use test_strategy::Arbitrary;

#[cfg(not(target_arch = "wasm32"))]
use chrono::NaiveDateTime;

use crate::doc_scalar;

/// A non-negative amount of tokens.
///
/// This is a fixed-point fraction, with [`Amount::DECIMAL_PLACES`] digits after the point.
/// [`Amount::ONE`] is one whole token, divisible into `10.pow(Amount::DECIMAL_PLACES)` parts.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug)]
pub struct Amount(u128);

#[derive(Serialize, Deserialize)]
#[serde(rename = "Amount")]
struct AmountString(String);

#[derive(Serialize, Deserialize)]
#[serde(rename = "Amount")]
struct AmountU128(u128);

impl Serialize for Amount {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            AmountString(self.to_string()).serialize(serializer)
        } else {
            AmountU128(self.0).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Amount {
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let AmountString(s) = AmountString::deserialize(deserializer)?;
            s.parse().map_err(serde::de::Error::custom)
        } else {
            Ok(Amount(AmountU128::deserialize(deserializer)?.0))
        }
    }
}

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

            pub fn saturating_mul(&self, other: $wrapped) -> Self {
                Self(self.0.saturating_mul(other))
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

impl_wrapped_number!(Amount, u128);
impl_wrapped_number!(BlockHeight, u64);
impl_strictly_wrapped_number!(RoundNumber, u64);

impl fmt::Display for Amount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Print the wrapped integer, padded with zeros to cover a digit before the decimal point.
        let places = Amount::DECIMAL_PLACES as usize;
        let min_digits = places + 1;
        let decimals = format!("{:0min_digits$}", self.0);
        let integer_part = &decimals[..(decimals.len() - places)];
        let fractional_part = decimals[(decimals.len() - places)..].trim_end_matches('0');

        // For now, we never trim non-zero digits so we don't lose any precision.
        let precision = f.precision().unwrap_or(0).max(fractional_part.len());
        let sign = if f.sign_plus() && self.0 > 0 { "+" } else { "" };
        // The amount of padding: desired width minus sign, point and number of digits.
        let pad_width = f.width().map_or(0, |w| {
            w.saturating_sub(precision)
                .saturating_sub(sign.len() + integer_part.len() + 1)
        });
        let left_pad = match f.align() {
            None | Some(fmt::Alignment::Right) => pad_width,
            Some(fmt::Alignment::Center) => pad_width / 2,
            Some(fmt::Alignment::Left) => 0,
        };

        for _ in 0..left_pad {
            write!(f, "{}", f.fill())?;
        }
        write!(f, "{sign}{integer_part}.{fractional_part:0<precision$}")?;
        for _ in left_pad..pad_width {
            write!(f, "{}", f.fill())?;
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum ParseAmountError {
    #[error("cannot parse amount")]
    Parse,
    #[error("cannot represent amount: number too high")]
    TooHigh,
    #[error("cannot represent amount: too many decimal places after the point")]
    TooManyDigits,
}

impl std::str::FromStr for Amount {
    type Err = ParseAmountError;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        let mut result: u128 = 0;
        let mut decimals: Option<u8> = None;
        let mut chars = src.trim().chars().peekable();
        if chars.peek() == Some(&'+') {
            chars.next();
        }
        for char in chars {
            match char {
                '_' => {}
                '.' if decimals.is_some() => return Err(ParseAmountError::Parse),
                '.' => decimals = Some(Amount::DECIMAL_PLACES),
                char => {
                    let digit = u128::from(char.to_digit(10).ok_or(ParseAmountError::Parse)?);
                    if let Some(d) = &mut decimals {
                        *d = d.checked_sub(1).ok_or(ParseAmountError::TooManyDigits)?;
                    }
                    result = result
                        .checked_mul(10)
                        .and_then(|r| r.checked_add(digit))
                        .ok_or(ParseAmountError::TooHigh)?;
                }
            }
        }
        result = result
            .checked_mul(10u128.pow(decimals.unwrap_or(Amount::DECIMAL_PLACES) as u32))
            .ok_or(ParseAmountError::TooHigh)?;
        Ok(Amount(result))
    }
}

impl fmt::Display for BlockHeight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for BlockHeight {
    type Err = std::num::ParseIntError;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        Ok(Self(u64::from_str(src)?))
    }
}

impl fmt::Display for RoundNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for RoundNumber {
    type Err = std::num::ParseIntError;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        Ok(Self(u64::from_str(src)?))
    }
}

impl<'a> std::iter::Sum<&'a Amount> for Amount {
    fn sum<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
        iter.fold(Self::zero(), |a, b| a.saturating_add(*b))
    }
}

impl Amount {
    pub const DECIMAL_PLACES: u8 = 18;
    pub const ONE: Amount = Amount(10u128.pow(Amount::DECIMAL_PLACES as u32));
    pub const ZERO: Amount = Amount(0);

    /// Helper function to obtain the 64 most significant bits of the balance.
    pub fn upper_half(self) -> u64 {
        (self.0 >> 64) as u64
    }

    /// Helper function to obtain the 64 least significant bits of the balance.
    pub fn lower_half(self) -> u64 {
        self.0 as u64
    }

    /// Divides this by the other amount. If the other is 0, it returns `u128::MAX`.
    pub fn saturating_div(self, other: Amount) -> u128 {
        self.0.checked_div(other.0).unwrap_or(u128::MAX)
    }
}

doc_scalar!(Amount, "A non-negative amount of tokens.");
doc_scalar!(BlockHeight, "A block height to identify blocks in a chain");
doc_scalar!(
    Timestamp,
    "A timestamp, in microseconds since the Unix epoch"
);

#[cfg(test)]
mod tests {
    use super::Amount;
    use std::str::FromStr;

    #[test]
    fn display_amount() {
        assert_eq!("1.", Amount::ONE.to_string());
        assert_eq!("1.", Amount::from_str("1.").unwrap().to_string());
        assert_eq!(
            Amount(10_000_000_000_000_000_000),
            Amount::from_str("10").unwrap()
        );
        assert_eq!("10.", Amount(10_000_000_000_000_000_000).to_string(),);
        assert_eq!(
            "1001.3",
            (Amount::from_str("1.1")
                .unwrap()
                .saturating_add(Amount::from_str("1_000.2").unwrap()))
            .to_string()
        );
        assert_eq!(
            "   1.00000000000000000000",
            format!("{:25.20}", Amount::ONE)
        );
        assert_eq!(
            "~+12.34~~",
            format!("{:~^+9.1}", Amount::from_str("12.34").unwrap())
        );
    }
}
