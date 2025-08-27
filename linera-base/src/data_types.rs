// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Core data-types used in the Linera protocol.

#[cfg(with_testing)]
use std::ops;
use std::{
    fmt::{self, Display},
    fs,
    hash::Hash,
    io, iter,
    num::ParseIntError,
    ops::{Bound, RangeBounds},
    path::Path,
    str::FromStr,
    sync::Arc,
};

use alloy_primitives::U256;
use async_graphql::{InputObject, SimpleObject};
use custom_debug_derive::Debug;
use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{serde_as, Bytes};
use thiserror::Error;

#[cfg(with_metrics)]
use crate::prometheus_util::MeasureLatency as _;
use crate::{
    crypto::{BcsHashable, CryptoError, CryptoHash},
    doc_scalar, hex_debug, http,
    identifiers::{
        ApplicationId, BlobId, BlobType, ChainId, EventId, GenericApplicationId, ModuleId, StreamId,
    },
    limited_writer::{LimitedWriter, LimitedWriterError},
    ownership::ChainOwnership,
    time::{Duration, SystemTime},
    vm::VmRuntime,
};

/// A non-negative amount of tokens.
///
/// This is a fixed-point fraction, with [`Amount::DECIMAL_PLACES`] digits after the point.
/// [`Amount::ONE`] is one whole token, divisible into `10.pow(Amount::DECIMAL_PLACES)` parts.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, WitType, WitLoad, WitStore,
)]
#[cfg_attr(
    all(with_testing, not(target_arch = "wasm32")),
    derive(test_strategy::Arbitrary)
)]
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

impl From<Amount> for U256 {
    fn from(amount: Amount) -> U256 {
        U256::from(amount.0)
    }
}

/// A block height to identify blocks in a chain.
#[derive(
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Copy,
    Clone,
    Hash,
    Default,
    Debug,
    Serialize,
    Deserialize,
    WitType,
    WitLoad,
    WitStore,
)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
pub struct BlockHeight(pub u64);

/// An identifier for successive attempts to decide a value in a consensus protocol.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
pub enum Round {
    /// The initial fast round.
    #[default]
    Fast,
    /// The N-th multi-leader round.
    MultiLeader(u32),
    /// The N-th single-leader round.
    SingleLeader(u32),
    /// The N-th round where the validators rotate as leaders.
    Validator(u32),
}

/// A duration in microseconds.
#[derive(
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Copy,
    Clone,
    Hash,
    Default,
    Debug,
    Serialize,
    Deserialize,
    WitType,
    WitLoad,
    WitStore,
)]
pub struct TimeDelta(u64);

impl TimeDelta {
    /// Returns the given number of microseconds as a [`TimeDelta`].
    pub const fn from_micros(micros: u64) -> Self {
        TimeDelta(micros)
    }

    /// Returns the given number of milliseconds as a [`TimeDelta`].
    pub const fn from_millis(millis: u64) -> Self {
        TimeDelta(millis.saturating_mul(1_000))
    }

    /// Returns the given number of seconds as a [`TimeDelta`].
    pub const fn from_secs(secs: u64) -> Self {
        TimeDelta(secs.saturating_mul(1_000_000))
    }

    /// Returns the given duration, rounded to the nearest microsecond and capped to the maximum
    /// [`TimeDelta`] value.
    pub fn from_duration(duration: Duration) -> Self {
        TimeDelta::from_micros(u64::try_from(duration.as_micros()).unwrap_or(u64::MAX))
    }

    /// Returns this [`TimeDelta`] as a number of microseconds.
    pub const fn as_micros(&self) -> u64 {
        self.0
    }

    /// Returns this [`TimeDelta`] as a [`Duration`].
    pub const fn as_duration(&self) -> Duration {
        Duration::from_micros(self.as_micros())
    }
}

/// A timestamp, in microseconds since the Unix epoch.
#[derive(
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Copy,
    Clone,
    Hash,
    Default,
    Debug,
    Serialize,
    Deserialize,
    WitType,
    WitLoad,
    WitStore,
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
    pub const fn micros(&self) -> u64 {
        self.0
    }

    /// Returns the [`TimeDelta`] between `other` and `self`, or zero if `other` is not earlier
    /// than `self`.
    pub const fn delta_since(&self, other: Timestamp) -> TimeDelta {
        TimeDelta::from_micros(self.0.saturating_sub(other.0))
    }

    /// Returns the [`Duration`] between `other` and `self`, or zero if `other` is not
    /// earlier than `self`.
    pub const fn duration_since(&self, other: Timestamp) -> Duration {
        Duration::from_micros(self.0.saturating_sub(other.0))
    }

    /// Returns the timestamp that is `duration` later than `self`.
    pub const fn saturating_add(&self, duration: TimeDelta) -> Timestamp {
        Timestamp(self.0.saturating_add(duration.0))
    }

    /// Returns the timestamp that is `duration` earlier than `self`.
    pub const fn saturating_sub(&self, duration: TimeDelta) -> Timestamp {
        Timestamp(self.0.saturating_sub(duration.0))
    }

    /// Returns a timestamp `micros` microseconds earlier than `self`, or the lowest possible value
    /// if it would underflow.
    pub const fn saturating_sub_micros(&self, micros: u64) -> Timestamp {
        Timestamp(self.0.saturating_sub(micros))
    }
}

impl From<u64> for Timestamp {
    fn from(t: u64) -> Timestamp {
        Timestamp(t)
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(date_time) = chrono::DateTime::from_timestamp(
            (self.0 / 1_000_000) as i64,
            ((self.0 % 1_000_000) * 1_000) as u32,
        ) {
            return date_time.naive_utc().fmt(f);
        }
        self.0.fmt(f)
    }
}

/// Resources that an application may spend during the execution of transaction or an
/// application call.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, WitLoad, WitStore, WitType,
)]
pub struct Resources {
    /// An amount of Wasm execution fuel.
    pub wasm_fuel: u64,
    /// An amount of EVM execution fuel.
    pub evm_fuel: u64,
    /// A number of read operations to be executed.
    pub read_operations: u32,
    /// A number of write operations to be executed.
    pub write_operations: u32,
    /// A number of bytes read from runtime.
    pub bytes_runtime: u32,
    /// A number of bytes to read.
    pub bytes_to_read: u32,
    /// A number of bytes to write.
    pub bytes_to_write: u32,
    /// A number of blobs to read.
    pub blobs_to_read: u32,
    /// A number of blobs to publish.
    pub blobs_to_publish: u32,
    /// A number of blob bytes to read.
    pub blob_bytes_to_read: u32,
    /// A number of blob bytes to publish.
    pub blob_bytes_to_publish: u32,
    /// A number of messages to be sent.
    pub messages: u32,
    /// The size of the messages to be sent.
    // TODO(#1531): Account for the type of message to be sent.
    pub message_size: u32,
    /// An increase in the amount of storage space.
    pub storage_size_delta: u32,
    /// A number of service-as-oracle requests to be performed.
    pub service_as_oracle_queries: u32,
    /// A number of HTTP requests to be performed.
    pub http_requests: u32,
    // TODO(#1532): Account for the system calls that we plan on calling.
    // TODO(#1533): Allow declaring calls to other applications instead of having to count them here.
}

/// A request to send a message.
#[derive(Clone, Debug, Deserialize, Serialize, WitLoad, WitType)]
#[cfg_attr(with_testing, derive(Eq, PartialEq, WitStore))]
#[witty_specialize_with(Message = Vec<u8>)]
pub struct SendMessageRequest<Message> {
    /// The destination of the message.
    pub destination: ChainId,
    /// Whether the message is authenticated.
    pub authenticated: bool,
    /// Whether the message is tracked.
    pub is_tracked: bool,
    /// The grant resources forwarded with the message.
    pub grant: Resources,
    /// The message itself.
    pub message: Message,
}

impl<Message> SendMessageRequest<Message>
where
    Message: Serialize,
{
    /// Serializes the internal `Message` type into raw bytes.
    pub fn into_raw(self) -> SendMessageRequest<Vec<u8>> {
        let message = bcs::to_bytes(&self.message).expect("Failed to serialize message");

        SendMessageRequest {
            destination: self.destination,
            authenticated: self.authenticated,
            is_tracked: self.is_tracked,
            grant: self.grant,
            message,
        }
    }
}

/// An error type for arithmetic errors.
#[derive(Debug, Error)]
#[allow(missing_docs)]
pub enum ArithmeticError {
    #[error("Number overflow")]
    Overflow,
    #[error("Number underflow")]
    Underflow,
}

macro_rules! impl_wrapped_number {
    ($name:ident, $wrapped:ident) => {
        impl $name {
            /// The zero value.
            pub const ZERO: Self = Self(0);

            /// The maximum value.
            pub const MAX: Self = Self($wrapped::MAX);

            /// Checked addition.
            pub fn try_add(self, other: Self) -> Result<Self, ArithmeticError> {
                let val = self
                    .0
                    .checked_add(other.0)
                    .ok_or(ArithmeticError::Overflow)?;
                Ok(Self(val))
            }

            /// Checked increment.
            pub fn try_add_one(self) -> Result<Self, ArithmeticError> {
                let val = self.0.checked_add(1).ok_or(ArithmeticError::Overflow)?;
                Ok(Self(val))
            }

            /// Saturating addition.
            pub const fn saturating_add(self, other: Self) -> Self {
                let val = self.0.saturating_add(other.0);
                Self(val)
            }

            /// Checked subtraction.
            pub fn try_sub(self, other: Self) -> Result<Self, ArithmeticError> {
                let val = self
                    .0
                    .checked_sub(other.0)
                    .ok_or(ArithmeticError::Underflow)?;
                Ok(Self(val))
            }

            /// Checked decrement.
            pub fn try_sub_one(self) -> Result<Self, ArithmeticError> {
                let val = self.0.checked_sub(1).ok_or(ArithmeticError::Underflow)?;
                Ok(Self(val))
            }

            /// Saturating subtraction.
            pub const fn saturating_sub(self, other: Self) -> Self {
                let val = self.0.saturating_sub(other.0);
                Self(val)
            }

            /// Checked in-place addition.
            pub fn try_add_assign(&mut self, other: Self) -> Result<(), ArithmeticError> {
                self.0 = self
                    .0
                    .checked_add(other.0)
                    .ok_or(ArithmeticError::Overflow)?;
                Ok(())
            }

            /// Checked in-place increment.
            pub fn try_add_assign_one(&mut self) -> Result<(), ArithmeticError> {
                self.0 = self.0.checked_add(1).ok_or(ArithmeticError::Overflow)?;
                Ok(())
            }

            /// Saturating in-place addition.
            pub const fn saturating_add_assign(&mut self, other: Self) {
                self.0 = self.0.saturating_add(other.0);
            }

            /// Checked in-place subtraction.
            pub fn try_sub_assign(&mut self, other: Self) -> Result<(), ArithmeticError> {
                self.0 = self
                    .0
                    .checked_sub(other.0)
                    .ok_or(ArithmeticError::Underflow)?;
                Ok(())
            }

            /// Saturating multiplication.
            pub const fn saturating_mul(&self, other: $wrapped) -> Self {
                Self(self.0.saturating_mul(other))
            }

            /// Checked multiplication.
            pub fn try_mul(self, other: $wrapped) -> Result<Self, ArithmeticError> {
                let val = self.0.checked_mul(other).ok_or(ArithmeticError::Overflow)?;
                Ok(Self(val))
            }

            /// Checked in-place multiplication.
            pub fn try_mul_assign(&mut self, other: $wrapped) -> Result<(), ArithmeticError> {
                self.0 = self.0.checked_mul(other).ok_or(ArithmeticError::Overflow)?;
                Ok(())
            }
        }

        impl From<$name> for $wrapped {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        // Cannot directly create values for a wrapped type, except for testing.
        #[cfg(with_testing)]
        impl From<$wrapped> for $name {
            fn from(value: $wrapped) -> Self {
                Self(value)
            }
        }

        #[cfg(with_testing)]
        impl ops::Add for $name {
            type Output = Self;

            fn add(self, other: Self) -> Self {
                Self(self.0 + other.0)
            }
        }

        #[cfg(with_testing)]
        impl ops::Sub for $name {
            type Output = Self;

            fn sub(self, other: Self) -> Self {
                Self(self.0 - other.0)
            }
        }

        #[cfg(with_testing)]
        impl ops::Mul<$wrapped> for $name {
            type Output = Self;

            fn mul(self, other: $wrapped) -> Self {
                Self(self.0 * other)
            }
        }
    };
}

impl TryFrom<BlockHeight> for usize {
    type Error = ArithmeticError;

    fn try_from(height: BlockHeight) -> Result<usize, ArithmeticError> {
        usize::try_from(height.0).map_err(|_| ArithmeticError::Overflow)
    }
}

/// Allows converting [`BlockHeight`] ranges to inclusive tuples of bounds.
pub trait BlockHeightRangeBounds {
    /// Returns the range as a tuple of inclusive bounds.
    /// If the range is empty, returns `None`.
    fn to_inclusive(&self) -> Option<(BlockHeight, BlockHeight)>;
}

impl<T: RangeBounds<BlockHeight>> BlockHeightRangeBounds for T {
    fn to_inclusive(&self) -> Option<(BlockHeight, BlockHeight)> {
        let start = match self.start_bound() {
            Bound::Included(height) => *height,
            Bound::Excluded(height) => height.try_add_one().ok()?,
            Bound::Unbounded => BlockHeight(0),
        };
        let end = match self.end_bound() {
            Bound::Included(height) => *height,
            Bound::Excluded(height) => height.try_sub_one().ok()?,
            Bound::Unbounded => BlockHeight::MAX,
        };
        if start > end {
            return None;
        }
        Some((start, end))
    }
}

impl_wrapped_number!(Amount, u128);
impl_wrapped_number!(BlockHeight, u64);
impl_wrapped_number!(TimeDelta, u64);

impl Display for Amount {
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
#[allow(missing_docs)]
pub enum ParseAmountError {
    #[error("cannot parse amount")]
    Parse,
    #[error("cannot represent amount: number too high")]
    TooHigh,
    #[error("cannot represent amount: too many decimal places after the point")]
    TooManyDigits,
}

impl FromStr for Amount {
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

impl Display for BlockHeight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for BlockHeight {
    type Err = ParseIntError;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        Ok(Self(u64::from_str(src)?))
    }
}

impl Display for Round {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Round::Fast => write!(f, "fast round"),
            Round::MultiLeader(r) => write!(f, "multi-leader round {}", r),
            Round::SingleLeader(r) => write!(f, "single-leader round {}", r),
            Round::Validator(r) => write!(f, "validator round {}", r),
        }
    }
}

impl Round {
    /// Whether the round is a multi-leader round.
    pub fn is_multi_leader(&self) -> bool {
        matches!(self, Round::MultiLeader(_))
    }

    /// Returns the round number if this is a multi-leader round, `None` otherwise.
    pub fn multi_leader(&self) -> Option<u32> {
        match self {
            Round::MultiLeader(number) => Some(*number),
            _ => None,
        }
    }

    /// Whether the round is the fast round.
    pub fn is_fast(&self) -> bool {
        matches!(self, Round::Fast)
    }

    /// The index of a round amongst the rounds of the same category.
    pub fn number(&self) -> u32 {
        match self {
            Round::Fast => 0,
            Round::MultiLeader(r) | Round::SingleLeader(r) | Round::Validator(r) => *r,
        }
    }

    /// The category of the round as a string.
    pub fn type_name(&self) -> &'static str {
        match self {
            Round::Fast => "fast",
            Round::MultiLeader(_) => "multi",
            Round::SingleLeader(_) => "single",
            Round::Validator(_) => "validator",
        }
    }
}

impl<'a> iter::Sum<&'a Amount> for Amount {
    fn sum<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
        iter.fold(Self::ZERO, |a, b| a.saturating_add(*b))
    }
}

impl Amount {
    /// The base-10 exponent representing how much a token can be divided.
    pub const DECIMAL_PLACES: u8 = 18;

    /// One token.
    pub const ONE: Amount = Amount(10u128.pow(Amount::DECIMAL_PLACES as u32));

    /// Returns an `Amount` corresponding to that many tokens, or `Amount::MAX` if saturated.
    pub const fn from_tokens(tokens: u128) -> Amount {
        Self::ONE.saturating_mul(tokens)
    }

    /// Returns an `Amount` corresponding to that many millitokens, or `Amount::MAX` if saturated.
    pub const fn from_millis(millitokens: u128) -> Amount {
        Amount(10u128.pow(Amount::DECIMAL_PLACES as u32 - 3)).saturating_mul(millitokens)
    }

    /// Returns an `Amount` corresponding to that many microtokens, or `Amount::MAX` if saturated.
    pub const fn from_micros(microtokens: u128) -> Amount {
        Amount(10u128.pow(Amount::DECIMAL_PLACES as u32 - 6)).saturating_mul(microtokens)
    }

    /// Returns an `Amount` corresponding to that many nanotokens, or `Amount::MAX` if saturated.
    pub const fn from_nanos(nanotokens: u128) -> Amount {
        Amount(10u128.pow(Amount::DECIMAL_PLACES as u32 - 9)).saturating_mul(nanotokens)
    }

    /// Returns an `Amount` corresponding to that many attotokens.
    pub const fn from_attos(attotokens: u128) -> Amount {
        Amount(attotokens)
    }

    /// Helper function to obtain the 64 most significant bits of the balance.
    pub const fn upper_half(self) -> u64 {
        (self.0 >> 64) as u64
    }

    /// Helper function to obtain the 64 least significant bits of the balance.
    pub const fn lower_half(self) -> u64 {
        self.0 as u64
    }

    /// Divides this by the other amount. If the other is 0, it returns `u128::MAX`.
    pub fn saturating_div(self, other: Amount) -> u128 {
        self.0.checked_div(other.0).unwrap_or(u128::MAX)
    }

    /// Returns whether this amount is 0.
    pub fn is_zero(&self) -> bool {
        *self == Amount::ZERO
    }
}

/// What created a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub enum ChainOrigin {
    /// The chain was created by the genesis configuration.
    Root(u32),
    /// The chain was created by a call from another chain.
    Child {
        /// The parent of this chain.
        parent: ChainId,
        /// The block height in the parent at which this chain was created.
        block_height: BlockHeight,
        /// The index of this chain among chains created at the same block height in the parent
        /// chain.
        chain_index: u32,
    },
}

impl ChainOrigin {
    /// Whether the chain was created by another chain.
    pub fn is_child(&self) -> bool {
        matches!(self, ChainOrigin::Child { .. })
    }
}

/// A number identifying the configuration of the chain (aka the committee).
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug)]
pub struct Epoch(pub u32);

impl Epoch {
    /// The zero epoch.
    pub const ZERO: Epoch = Epoch(0);
}

impl Serialize for Epoch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.0.to_string())
        } else {
            serializer.serialize_newtype_struct("Epoch", &self.0)
        }
    }
}

impl<'de> Deserialize<'de> for Epoch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            Ok(Epoch(u32::from_str(&s).map_err(serde::de::Error::custom)?))
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "Epoch")]
            struct EpochDerived(u32);

            let value = EpochDerived::deserialize(deserializer)?;
            Ok(Self(value.0))
        }
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Epoch {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Epoch(s.parse()?))
    }
}

impl From<u32> for Epoch {
    fn from(value: u32) -> Self {
        Epoch(value)
    }
}

impl Epoch {
    /// Tries to return an epoch with a number increased by one. Returns an error if an overflow
    /// happens.
    #[inline]
    pub fn try_add_one(self) -> Result<Self, ArithmeticError> {
        let val = self.0.checked_add(1).ok_or(ArithmeticError::Overflow)?;
        Ok(Self(val))
    }

    /// Tries to return an epoch with a number decreased by one. Returns an error if an underflow
    /// happens.
    pub fn try_sub_one(self) -> Result<Self, ArithmeticError> {
        let val = self.0.checked_sub(1).ok_or(ArithmeticError::Underflow)?;
        Ok(Self(val))
    }

    /// Tries to add one to this epoch's number. Returns an error if an overflow happens.
    #[inline]
    pub fn try_add_assign_one(&mut self) -> Result<(), ArithmeticError> {
        self.0 = self.0.checked_add(1).ok_or(ArithmeticError::Overflow)?;
        Ok(())
    }
}

/// The initial configuration for a new chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct InitialChainConfig {
    /// The ownership configuration of the new chain.
    pub ownership: ChainOwnership,
    /// The epoch in which the chain is created.
    pub epoch: Epoch,
    /// The lowest number of an active epoch at the time of creation of the chain.
    pub min_active_epoch: Epoch,
    /// The highest number of an active epoch at the time of creation of the chain.
    pub max_active_epoch: Epoch,
    /// The initial chain balance.
    pub balance: Amount,
    /// The initial application permissions.
    pub application_permissions: ApplicationPermissions,
}

/// Initial chain configuration and chain origin.
#[derive(Eq, PartialEq, Clone, Hash, Debug, Serialize, Deserialize)]
pub struct ChainDescription {
    origin: ChainOrigin,
    timestamp: Timestamp,
    config: InitialChainConfig,
}

impl ChainDescription {
    /// Creates a new [`ChainDescription`].
    pub fn new(origin: ChainOrigin, config: InitialChainConfig, timestamp: Timestamp) -> Self {
        Self {
            origin,
            config,
            timestamp,
        }
    }

    /// Returns the [`ChainId`] based on this [`ChainDescription`].
    pub fn id(&self) -> ChainId {
        ChainId::from(self)
    }

    /// Returns the [`ChainOrigin`] describing who created this chain.
    pub fn origin(&self) -> ChainOrigin {
        self.origin
    }

    /// Returns a reference to the [`InitialChainConfig`] of the chain.
    pub fn config(&self) -> &InitialChainConfig {
        &self.config
    }

    /// Returns the timestamp of when the chain was created.
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Whether the chain was created by another chain.
    pub fn is_child(&self) -> bool {
        self.origin.is_child()
    }
}

impl BcsHashable<'_> for ChainDescription {}

/// A description of the current Linera network to be stored in every node's database.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkDescription {
    /// The name of the network.
    pub name: String,
    /// Hash of the network's genesis config.
    pub genesis_config_hash: CryptoHash,
    /// Genesis timestamp.
    pub genesis_timestamp: Timestamp,
    /// Hash of the blob containing the genesis committee.
    pub genesis_committee_blob_hash: CryptoHash,
    /// The chain ID of the admin chain.
    pub admin_chain_id: ChainId,
}

/// Permissions for applications on a chain.
#[derive(
    Default,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Clone,
    Serialize,
    Deserialize,
    WitType,
    WitLoad,
    WitStore,
    InputObject,
)]
pub struct ApplicationPermissions {
    /// If this is `None`, all system operations and application operations are allowed.
    /// If it is `Some`, only operations from the specified applications are allowed, and
    /// no system operations.
    #[debug(skip_if = Option::is_none)]
    pub execute_operations: Option<Vec<ApplicationId>>,
    /// At least one operation or incoming message from each of these applications must occur in
    /// every block.
    #[graphql(default)]
    #[debug(skip_if = Vec::is_empty)]
    pub mandatory_applications: Vec<ApplicationId>,
    /// These applications are allowed to close the current chain.
    #[graphql(default)]
    #[debug(skip_if = Vec::is_empty)]
    pub close_chain: Vec<ApplicationId>,
    /// These applications are allowed to change the application permissions.
    #[graphql(default)]
    #[debug(skip_if = Vec::is_empty)]
    pub change_application_permissions: Vec<ApplicationId>,
    /// These applications are allowed to perform calls to services as oracles.
    #[graphql(default)]
    #[debug(skip_if = Option::is_none)]
    pub call_service_as_oracle: Option<Vec<ApplicationId>>,
    /// These applications are allowed to perform HTTP requests.
    #[graphql(default)]
    #[debug(skip_if = Option::is_none)]
    pub make_http_requests: Option<Vec<ApplicationId>>,
}

impl ApplicationPermissions {
    /// Creates new `ApplicationPermissions` where the given application is the only one
    /// whose operations are allowed and mandatory, and it can also close the chain.
    pub fn new_single(app_id: ApplicationId) -> Self {
        Self {
            execute_operations: Some(vec![app_id]),
            mandatory_applications: vec![app_id],
            close_chain: vec![app_id],
            change_application_permissions: vec![app_id],
            call_service_as_oracle: Some(vec![app_id]),
            make_http_requests: Some(vec![app_id]),
        }
    }

    /// Creates new `ApplicationPermissions` where the given applications are the only ones
    /// whose operations are allowed and mandatory, and they can also close the chain.
    pub fn new_multiple(app_ids: Vec<ApplicationId>) -> Self {
        Self {
            execute_operations: Some(app_ids.clone()),
            mandatory_applications: app_ids.clone(),
            close_chain: app_ids.clone(),
            change_application_permissions: app_ids.clone(),
            call_service_as_oracle: Some(app_ids.clone()),
            make_http_requests: Some(app_ids),
        }
    }

    /// Returns whether operations with the given application ID are allowed on this chain.
    pub fn can_execute_operations(&self, app_id: &GenericApplicationId) -> bool {
        match (app_id, &self.execute_operations) {
            (_, None) => true,
            (GenericApplicationId::System, Some(_)) => false,
            (GenericApplicationId::User(app_id), Some(app_ids)) => app_ids.contains(app_id),
        }
    }

    /// Returns whether the given application is allowed to close this chain.
    pub fn can_close_chain(&self, app_id: &ApplicationId) -> bool {
        self.close_chain.contains(app_id)
    }

    /// Returns whether the given application is allowed to change the application
    /// permissions for this chain.
    pub fn can_change_application_permissions(&self, app_id: &ApplicationId) -> bool {
        self.change_application_permissions.contains(app_id)
    }

    /// Returns whether the given application can call services.
    pub fn can_call_services(&self, app_id: &ApplicationId) -> bool {
        self.call_service_as_oracle
            .as_ref()
            .map(|app_ids| app_ids.contains(app_id))
            .unwrap_or(true)
    }

    /// Returns whether the given application can make HTTP requests.
    pub fn can_make_http_requests(&self, app_id: &ApplicationId) -> bool {
        self.make_http_requests
            .as_ref()
            .map(|app_ids| app_ids.contains(app_id))
            .unwrap_or(true)
    }
}

/// A record of a single oracle response.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum OracleResponse {
    /// The response from a service query.
    Service(
        #[debug(with = "hex_debug")]
        #[serde(with = "serde_bytes")]
        Vec<u8>,
    ),
    /// The response from an HTTP request.
    Http(http::Response),
    /// A successful read or write of a blob.
    Blob(BlobId),
    /// An assertion oracle that passed.
    Assert,
    /// The block's validation round.
    Round(Option<u32>),
    /// An event was read.
    Event(EventId, Vec<u8>),
    /// An event exists.
    EventExists(EventId),
}

impl BcsHashable<'_> for OracleResponse {}

/// Description of a user application.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Hash, Serialize)]
pub struct ApplicationDescription {
    /// The unique ID of the bytecode to use for the application.
    pub module_id: ModuleId,
    /// The chain ID that created the application.
    pub creator_chain_id: ChainId,
    /// Height of the block that created this application.
    pub block_height: BlockHeight,
    /// The index of the application among those created in the same block.
    pub application_index: u32,
    /// The parameters of the application.
    #[serde(with = "serde_bytes")]
    #[debug(with = "hex_debug")]
    pub parameters: Vec<u8>,
    /// Required dependencies.
    pub required_application_ids: Vec<ApplicationId>,
}

impl From<&ApplicationDescription> for ApplicationId {
    fn from(description: &ApplicationDescription) -> Self {
        let mut hash = CryptoHash::new(&BlobContent::new_application_description(description));
        if matches!(description.module_id.vm_runtime, VmRuntime::Evm) {
            hash.make_evm_compatible();
        }
        ApplicationId::new(hash)
    }
}

impl BcsHashable<'_> for ApplicationDescription {}

impl ApplicationDescription {
    /// Gets the serialized bytes for this `ApplicationDescription`.
    pub fn to_bytes(&self) -> Vec<u8> {
        bcs::to_bytes(self).expect("Serializing blob bytes should not fail!")
    }

    /// Gets the `BlobId` of the contract
    pub fn contract_bytecode_blob_id(&self) -> BlobId {
        self.module_id.contract_bytecode_blob_id()
    }

    /// Gets the `BlobId` of the service
    pub fn service_bytecode_blob_id(&self) -> BlobId {
        self.module_id.service_bytecode_blob_id()
    }
}

/// A WebAssembly module's bytecode.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize, WitType, WitLoad, WitStore)]
pub struct Bytecode {
    /// Bytes of the bytecode.
    #[serde(with = "serde_bytes")]
    #[debug(with = "hex_debug")]
    pub bytes: Vec<u8>,
}

impl Bytecode {
    /// Creates a new [`Bytecode`] instance using the provided `bytes`.
    pub fn new(bytes: Vec<u8>) -> Self {
        Bytecode { bytes }
    }

    /// Load bytecode from a Wasm module file.
    pub async fn load_from_file(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let bytes = fs::read(path)?;
        Ok(Bytecode { bytes })
    }

    /// Compresses the [`Bytecode`] into a [`CompressedBytecode`].
    #[cfg(not(target_arch = "wasm32"))]
    pub fn compress(&self) -> CompressedBytecode {
        #[cfg(with_metrics)]
        let _compression_latency = metrics::BYTECODE_COMPRESSION_LATENCY.measure_latency();
        let compressed_bytes_vec = zstd::stream::encode_all(&*self.bytes, 19)
            .expect("Compressing bytes in memory should not fail");

        CompressedBytecode {
            compressed_bytes: Arc::new(compressed_bytes_vec.into_boxed_slice()),
        }
    }

    /// Compresses the [`Bytecode`] into a [`CompressedBytecode`].
    #[cfg(target_arch = "wasm32")]
    pub fn compress(&self) -> CompressedBytecode {
        use ruzstd::encoding::{CompressionLevel, FrameCompressor};

        #[cfg(with_metrics)]
        let _compression_latency = metrics::BYTECODE_COMPRESSION_LATENCY.measure_latency();

        let mut compressed_bytes_vec = Vec::new();
        let mut compressor = FrameCompressor::new(CompressionLevel::Fastest);
        compressor.set_source(&*self.bytes);
        compressor.set_drain(&mut compressed_bytes_vec);
        compressor.compress();

        CompressedBytecode {
            compressed_bytes: Arc::new(compressed_bytes_vec.into_boxed_slice()),
        }
    }
}

impl AsRef<[u8]> for Bytecode {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

/// A type for errors happening during decompression.
#[derive(Error, Debug)]
pub enum DecompressionError {
    /// Compressed bytecode is invalid, and could not be decompressed.
    #[error("Bytecode could not be decompressed: {0}")]
    InvalidCompressedBytecode(#[from] io::Error),
}

/// A compressed module bytecode (WebAssembly or EVM).
#[serde_as]
#[derive(Clone, Debug, Deserialize, Hash, Serialize, WitType, WitStore)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct CompressedBytecode {
    /// Compressed bytes of the bytecode.
    #[serde_as(as = "Arc<Bytes>")]
    #[debug(skip)]
    pub compressed_bytes: Arc<Box<[u8]>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl CompressedBytecode {
    /// Returns `true` if the decompressed size does not exceed the limit.
    pub fn decompressed_size_at_most(
        compressed_bytes: &[u8],
        limit: u64,
    ) -> Result<bool, DecompressionError> {
        let mut decoder = zstd::stream::Decoder::new(compressed_bytes)?;
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let mut writer = LimitedWriter::new(io::sink(), limit);
        match io::copy(&mut decoder, &mut writer) {
            Ok(_) => Ok(true),
            Err(error) => {
                error.downcast::<LimitedWriterError>()?;
                Ok(false)
            }
        }
    }

    /// Decompresses a [`CompressedBytecode`] into a [`Bytecode`].
    pub fn decompress(&self) -> Result<Bytecode, DecompressionError> {
        #[cfg(with_metrics)]
        let _decompression_latency = metrics::BYTECODE_DECOMPRESSION_LATENCY.measure_latency();
        let bytes = zstd::stream::decode_all(&**self.compressed_bytes)?;

        Ok(Bytecode { bytes })
    }
}

#[cfg(target_arch = "wasm32")]
impl CompressedBytecode {
    /// Returns `true` if the decompressed size does not exceed the limit.
    pub fn decompressed_size_at_most(
        compressed_bytes: &[u8],
        limit: u64,
    ) -> Result<bool, DecompressionError> {
        use ruzstd::decoding::StreamingDecoder;
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let mut writer = LimitedWriter::new(io::sink(), limit);
        let mut decoder = StreamingDecoder::new(compressed_bytes).map_err(io::Error::other)?;

        // TODO(#2710): Decode multiple frames, if present
        match io::copy(&mut decoder, &mut writer) {
            Ok(_) => Ok(true),
            Err(error) => {
                error.downcast::<LimitedWriterError>()?;
                Ok(false)
            }
        }
    }

    /// Decompresses a [`CompressedBytecode`] into a [`Bytecode`].
    pub fn decompress(&self) -> Result<Bytecode, DecompressionError> {
        use ruzstd::{decoding::StreamingDecoder, io::Read};

        #[cfg(with_metrics)]
        let _decompression_latency = BYTECODE_DECOMPRESSION_LATENCY.measure_latency();

        let compressed_bytes = &*self.compressed_bytes;
        let mut bytes = Vec::new();
        let mut decoder = StreamingDecoder::new(&**compressed_bytes).map_err(io::Error::other)?;

        // TODO(#2710): Decode multiple frames, if present
        while !decoder.get_ref().is_empty() {
            decoder
                .read_to_end(&mut bytes)
                .expect("Reading from a slice in memory should not result in I/O errors");
        }

        Ok(Bytecode { bytes })
    }
}

impl BcsHashable<'_> for BlobContent {}

/// A blob of binary data.
#[serde_as]
#[derive(Hash, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobContent {
    /// The type of data represented by the bytes.
    blob_type: BlobType,
    /// The binary data.
    #[debug(skip)]
    #[serde_as(as = "Arc<Bytes>")]
    bytes: Arc<Box<[u8]>>,
}

impl BlobContent {
    /// Creates a new [`BlobContent`] from the provided bytes and [`BlobId`].
    pub fn new(blob_type: BlobType, bytes: impl Into<Box<[u8]>>) -> Self {
        let bytes = bytes.into();
        BlobContent {
            blob_type,
            bytes: Arc::new(bytes),
        }
    }

    /// Creates a new data [`BlobContent`] from the provided bytes.
    pub fn new_data(bytes: impl Into<Box<[u8]>>) -> Self {
        BlobContent::new(BlobType::Data, bytes)
    }

    /// Creates a new contract bytecode [`BlobContent`] from the provided bytes.
    pub fn new_contract_bytecode(compressed_bytecode: CompressedBytecode) -> Self {
        BlobContent {
            blob_type: BlobType::ContractBytecode,
            bytes: compressed_bytecode.compressed_bytes,
        }
    }

    /// Creates a new contract bytecode [`BlobContent`] from the provided bytes.
    pub fn new_evm_bytecode(compressed_bytecode: CompressedBytecode) -> Self {
        BlobContent {
            blob_type: BlobType::EvmBytecode,
            bytes: compressed_bytecode.compressed_bytes,
        }
    }

    /// Creates a new service bytecode [`BlobContent`] from the provided bytes.
    pub fn new_service_bytecode(compressed_bytecode: CompressedBytecode) -> Self {
        BlobContent {
            blob_type: BlobType::ServiceBytecode,
            bytes: compressed_bytecode.compressed_bytes,
        }
    }

    /// Creates a new application description [`BlobContent`] from a [`ApplicationDescription`].
    pub fn new_application_description(application_description: &ApplicationDescription) -> Self {
        let bytes = application_description.to_bytes();
        BlobContent::new(BlobType::ApplicationDescription, bytes)
    }

    /// Creates a new committee [`BlobContent`] from the provided serialized committee.
    pub fn new_committee(committee: impl Into<Box<[u8]>>) -> Self {
        BlobContent::new(BlobType::Committee, committee)
    }

    /// Creates a new chain description [`BlobContent`] from a [`ChainDescription`].
    pub fn new_chain_description(chain_description: &ChainDescription) -> Self {
        let bytes = bcs::to_bytes(&chain_description)
            .expect("Serializing a ChainDescription should not fail!");
        BlobContent::new(BlobType::ChainDescription, bytes)
    }

    /// Gets a reference to the blob's bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Convert a BlobContent into `Vec<u8>` without cloning if possible.
    pub fn into_vec_or_clone(self) -> Vec<u8> {
        let bytes = Arc::unwrap_or_clone(self.bytes);
        bytes.into_vec()
    }

    /// Get the Arc<Box<[u8]>> directly without cloning.
    pub fn into_arc_bytes(self) -> Arc<Box<[u8]>> {
        self.bytes
    }

    /// Returns the type of data represented by this blob's bytes.
    pub fn blob_type(&self) -> BlobType {
        self.blob_type
    }
}

impl From<Blob> for BlobContent {
    fn from(blob: Blob) -> BlobContent {
        blob.content
    }
}

/// A blob of binary data, with its hash.
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct Blob {
    /// ID of the blob.
    hash: CryptoHash,
    /// A blob of binary data.
    content: BlobContent,
}

impl Blob {
    /// Computes the hash and returns the hashed blob for the given content.
    pub fn new(content: BlobContent) -> Self {
        let mut hash = CryptoHash::new(&content);
        if matches!(content.blob_type, BlobType::ApplicationDescription) {
            let application_description = bcs::from_bytes::<ApplicationDescription>(&content.bytes)
                .expect("to obtain an application description");
            if matches!(application_description.module_id.vm_runtime, VmRuntime::Evm) {
                hash.make_evm_compatible();
            }
        }
        Blob { hash, content }
    }

    /// Creates a blob from ud and content without checks
    pub fn new_with_hash_unchecked(blob_id: BlobId, content: BlobContent) -> Self {
        Blob {
            hash: blob_id.hash,
            content,
        }
    }

    /// Creates a blob without checking that the hash actually matches the content.
    pub fn new_with_id_unchecked(blob_id: BlobId, bytes: impl Into<Box<[u8]>>) -> Self {
        let bytes = bytes.into();
        Blob {
            hash: blob_id.hash,
            content: BlobContent {
                blob_type: blob_id.blob_type,
                bytes: Arc::new(bytes),
            },
        }
    }

    /// Creates a new data [`Blob`] from the provided bytes.
    pub fn new_data(bytes: impl Into<Box<[u8]>>) -> Self {
        Blob::new(BlobContent::new_data(bytes))
    }

    /// Creates a new contract bytecode [`Blob`] from the provided bytes.
    pub fn new_contract_bytecode(compressed_bytecode: CompressedBytecode) -> Self {
        Blob::new(BlobContent::new_contract_bytecode(compressed_bytecode))
    }

    /// Creates a new contract bytecode [`BlobContent`] from the provided bytes.
    pub fn new_evm_bytecode(compressed_bytecode: CompressedBytecode) -> Self {
        Blob::new(BlobContent::new_evm_bytecode(compressed_bytecode))
    }

    /// Creates a new service bytecode [`Blob`] from the provided bytes.
    pub fn new_service_bytecode(compressed_bytecode: CompressedBytecode) -> Self {
        Blob::new(BlobContent::new_service_bytecode(compressed_bytecode))
    }

    /// Creates a new application description [`Blob`] from the provided description.
    pub fn new_application_description(application_description: &ApplicationDescription) -> Self {
        Blob::new(BlobContent::new_application_description(
            application_description,
        ))
    }

    /// Creates a new committee [`Blob`] from the provided bytes.
    pub fn new_committee(committee: impl Into<Box<[u8]>>) -> Self {
        Blob::new(BlobContent::new_committee(committee))
    }

    /// Creates a new chain description [`Blob`] from a [`ChainDescription`].
    pub fn new_chain_description(chain_description: &ChainDescription) -> Self {
        Blob::new(BlobContent::new_chain_description(chain_description))
    }

    /// A content-addressed blob ID i.e. the hash of the `Blob`.
    pub fn id(&self) -> BlobId {
        BlobId {
            hash: self.hash,
            blob_type: self.content.blob_type,
        }
    }

    /// Returns a reference to the inner `BlobContent`, without the hash.
    pub fn content(&self) -> &BlobContent {
        &self.content
    }

    /// Moves ownership of the blob of binary data
    pub fn into_content(self) -> BlobContent {
        self.content
    }

    /// Gets a reference to the inner blob's bytes.
    pub fn bytes(&self) -> &[u8] {
        self.content.bytes()
    }

    /// Loads data blob from a file.
    pub async fn load_data_blob_from_file(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self::new_data(fs::read(path)?))
    }

    /// Returns whether the blob is of [`BlobType::Committee`] variant.
    pub fn is_committee_blob(&self) -> bool {
        self.content().blob_type().is_committee_blob()
    }
}

impl Serialize for Blob {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            let blob_bytes = bcs::to_bytes(&self.content).map_err(serde::ser::Error::custom)?;
            serializer.serialize_str(&hex::encode(blob_bytes))
        } else {
            BlobContent::serialize(self.content(), serializer)
        }
    }
}

impl<'a> Deserialize<'a> for Blob {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let content_bytes = hex::decode(s).map_err(serde::de::Error::custom)?;
            let content: BlobContent =
                bcs::from_bytes(&content_bytes).map_err(serde::de::Error::custom)?;

            Ok(Blob::new(content))
        } else {
            let content = BlobContent::deserialize(deserializer)?;
            Ok(Blob::new(content))
        }
    }
}

impl BcsHashable<'_> for Blob {}

/// An event recorded in a block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct Event {
    /// The ID of the stream this event belongs to.
    pub stream_id: StreamId,
    /// The event index, i.e. the number of events in the stream before this one.
    pub index: u32,
    /// The payload data.
    #[debug(with = "hex_debug")]
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
}

impl Event {
    /// Returns the ID of this event record, given the publisher chain ID.
    pub fn id(&self, chain_id: ChainId) -> EventId {
        EventId {
            chain_id,
            stream_id: self.stream_id.clone(),
            index: self.index,
        }
    }
}

/// An update for a stream with new events.
#[derive(Clone, Debug, Serialize, Deserialize, WitType, WitLoad, WitStore)]
pub struct StreamUpdate {
    /// The publishing chain.
    pub chain_id: ChainId,
    /// The stream ID.
    pub stream_id: StreamId,
    /// The lowest index of a new event. See [`StreamUpdate::new_indices`].
    pub previous_index: u32,
    /// The index of the next event, i.e. the lowest for which no event is known yet.
    pub next_index: u32,
}

impl StreamUpdate {
    /// Returns the indices of all new events in the stream.
    pub fn new_indices(&self) -> impl Iterator<Item = u32> {
        self.previous_index..self.next_index
    }
}

impl BcsHashable<'_> for Event {}

doc_scalar!(Bytecode, "A module bytecode (WebAssembly or EVM)");
doc_scalar!(Amount, "A non-negative amount of tokens.");
doc_scalar!(
    Epoch,
    "A number identifying the configuration of the chain (aka the committee)"
);
doc_scalar!(BlockHeight, "A block height to identify blocks in a chain");
doc_scalar!(
    Timestamp,
    "A timestamp, in microseconds since the Unix epoch"
);
doc_scalar!(TimeDelta, "A duration in microseconds");
doc_scalar!(
    Round,
    "A number to identify successive attempts to decide a value in a consensus protocol."
);
doc_scalar!(
    ChainDescription,
    "Initial chain configuration and chain origin."
);
doc_scalar!(OracleResponse, "A record of a single oracle response.");
doc_scalar!(BlobContent, "A blob of binary data.");
doc_scalar!(
    Blob,
    "A blob of binary data, with its content-addressed blob ID."
);
doc_scalar!(ApplicationDescription, "Description of a user application");

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use prometheus::HistogramVec;

    use crate::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};

    /// The time it takes to compress a bytecode.
    pub static BYTECODE_COMPRESSION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "bytecode_compression_latency",
            "Bytecode compression latency",
            &[],
            exponential_bucket_latencies(10.0),
        )
    });

    /// The time it takes to decompress a bytecode.
    pub static BYTECODE_DECOMPRESSION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "bytecode_decompression_latency",
            "Bytecode decompression latency",
            &[],
            exponential_bucket_latencies(10.0),
        )
    });
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{Amount, BlobContent};
    use crate::identifiers::BlobType;

    #[test]
    fn display_amount() {
        assert_eq!("1.", Amount::ONE.to_string());
        assert_eq!("1.", Amount::from_str("1.").unwrap().to_string());
        assert_eq!(
            Amount(10_000_000_000_000_000_000),
            Amount::from_str("10").unwrap()
        );
        assert_eq!("10.", Amount(10_000_000_000_000_000_000).to_string());
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

    #[test]
    fn blob_content_serialization_deserialization() {
        let test_data = b"Hello, world!".as_slice();
        let original_blob = BlobContent::new(BlobType::Data, test_data);

        let serialized = bcs::to_bytes(&original_blob).expect("Failed to serialize BlobContent");
        let deserialized: BlobContent =
            bcs::from_bytes(&serialized).expect("Failed to deserialize BlobContent");
        assert_eq!(original_blob, deserialized);

        let serialized =
            serde_json::to_vec(&original_blob).expect("Failed to serialize BlobContent");
        let deserialized: BlobContent =
            serde_json::from_slice(&serialized).expect("Failed to deserialize BlobContent");
        assert_eq!(original_blob, deserialized);
    }

    #[test]
    fn blob_content_hash_consistency() {
        let test_data = b"Hello, world!";
        let blob1 = BlobContent::new(BlobType::Data, test_data.as_slice());
        let blob2 = BlobContent::new(BlobType::Data, Vec::from(test_data.as_slice()));

        // Both should have same hash since they contain the same data
        let hash1 = crate::crypto::CryptoHash::new(&blob1);
        let hash2 = crate::crypto::CryptoHash::new(&blob2);

        assert_eq!(hash1, hash2, "Hashes should be equal for same content");
        assert_eq!(blob1.bytes(), blob2.bytes(), "Byte content should be equal");
    }
}
