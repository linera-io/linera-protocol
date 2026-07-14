// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Core data-types used in the Linera protocol.

#[cfg(with_testing)]
use std::ops;
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::{self, Display},
    fs,
    hash::Hash,
    io, iter,
    num::ParseIntError,
    str::FromStr,
    sync::Arc,
};

use allocative::{Allocative, Visitor};
use alloy_primitives::U256;
use async_graphql::{InputObject, SimpleObject};
use custom_debug_derive::Debug;
use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_name::{DeserializeNameAdapter, SerializeNameAdapter};
use serde_with::{serde_as, Bytes};
use thiserror::Error;
use tracing::instrument;

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

/// A [`BTreeMap`] that serializes like a `Vec<(K, V)>` instead of using BCS's canonical
/// map encoding.
///
/// BCS serializes a [`BTreeMap`] in *canonical* form: on every `serialize` call it re-sorts the
/// entries by their serialized-key bytes (an `O(n log n)` sort) and verifies that ordering again
/// on `deserialize`. Since a [`BTreeMap`] already keeps its entries ordered, this is wasted work.
/// `NonCanonicalBTreeMap` instead (de)serializes the entries as a plain sequence of pairs, exactly
/// like `Vec<(K, V)>`, trading the canonical wire format for speed.
///
/// Use it in *value* position — the value of a `RegisterView<Value>` or `MapView<_, Value>` — so
/// that `save()` does not pay the canonical sort. Never use it in *key* position
/// (`MapView<Key, _>`): keys rely on the canonical encoding that this type skips, so use
/// [`CanonicalBTreeMap`] there instead.
///
/// It otherwise behaves like a [`BTreeMap`]: it derefs to one, so all the usual methods are
/// available.
#[derive(Debug, Clone, PartialEq, Eq, Allocative)]
pub struct NonCanonicalBTreeMap<K, V>(BTreeMap<K, V>);

impl<K, V> Default for NonCanonicalBTreeMap<K, V> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl<K, V> std::ops::Deref for NonCanonicalBTreeMap<K, V> {
    type Target = BTreeMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> std::ops::DerefMut for NonCanonicalBTreeMap<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K, V> From<BTreeMap<K, V>> for NonCanonicalBTreeMap<K, V> {
    fn from(map: BTreeMap<K, V>) -> Self {
        Self(map)
    }
}

impl<K, V> From<NonCanonicalBTreeMap<K, V>> for BTreeMap<K, V> {
    fn from(map: NonCanonicalBTreeMap<K, V>) -> Self {
        map.0
    }
}

impl<K: Ord, V> FromIterator<(K, V)> for NonCanonicalBTreeMap<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self(BTreeMap::from_iter(iter))
    }
}

impl<K, V> IntoIterator for NonCanonicalBTreeMap<K, V> {
    type Item = (K, V);
    type IntoIter = std::collections::btree_map::IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, K, V> IntoIterator for &'a NonCanonicalBTreeMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = std::collections::btree_map::Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<K, V> Serialize for NonCanonicalBTreeMap<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Serialize as a sequence of pairs, exactly like `Vec<(K, V)>`. The entries are already
        // in key order, so this avoids the canonical re-sorting that BCS does for maps.
        serializer.collect_seq(self.0.iter())
    }
}

impl<'de, K, V> Deserialize<'de> for NonCanonicalBTreeMap<K, V>
where
    K: Deserialize<'de> + Ord,
    V: Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entries = Vec::<(K, V)>::deserialize(deserializer)?;
        Ok(Self(entries.into_iter().collect()))
    }
}

impl<K, V> async_graphql::OutputType for NonCanonicalBTreeMap<K, V>
where
    BTreeMap<K, V>: async_graphql::OutputType,
{
    fn type_name() -> std::borrow::Cow<'static, str> {
        <BTreeMap<K, V> as async_graphql::OutputType>::type_name()
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        <BTreeMap<K, V> as async_graphql::OutputType>::create_type_info(registry)
    }

    async fn resolve(
        &self,
        ctx: &async_graphql::ContextSelectionSet<'_>,
        field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
    ) -> async_graphql::ServerResult<async_graphql::Value> {
        self.0.resolve(ctx, field).await
    }
}

/// A [`BTreeSet`] used in value position; the counterpart to [`NonCanonicalBTreeMap`].
///
/// Unlike maps, serde already serializes a [`BTreeSet`] as a plain sequence (it never goes through
/// `serialize_map`), so BCS does not re-sort it. A type alias is therefore enough; no wrapper is
/// needed.
///
/// Use it in *value* position (`RegisterView<Value>` or `MapView<_, Value>`). In *key* position
/// (`MapView<Key, _>`) use [`CanonicalBTreeSet`] instead, which enforces the canonical ordering
/// that keys require.
pub type NonCanonicalBTreeSet<T> = BTreeSet<T>;

/// A [`BTreeMap`] suitable for *key* position; an alias for [`BTreeMap`] itself.
///
/// In key position the canonical BCS encoding is exactly what is wanted — keys are ordered and
/// compared by their serialized bytes — so no wrapper is needed. Use it for the key type of a
/// `MapView<Key, _>`. In *value* position prefer [`NonCanonicalBTreeMap`], which skips the
/// per-`save()` canonical sort. This alias exists to make that intent explicit and to pair with
/// [`NonCanonicalBTreeMap`].
pub type CanonicalBTreeMap<K, V> = BTreeMap<K, V>;

/// A [`BTreeSet`] that serializes canonically, like a `BTreeMap<T, ()>`.
///
/// A plain [`BTreeSet`] serializes as a serde *sequence*, so BCS keeps the in-memory (Rust `Ord`)
/// order without enforcing canonical ordering of the serialized elements. That is fine in value
/// position, but in *key* position the canonical encoding matters. `CanonicalBTreeSet` therefore
/// (de)serializes through a map of `T -> ()`, so that BCS sorts the elements by their serialized
/// bytes, exactly as it does for [`BTreeMap`] keys.
///
/// Use it for the key type of a `MapView<Key, _>`. In *value* position use
/// [`NonCanonicalBTreeSet`] instead. It otherwise behaves like a [`BTreeSet`]: it derefs to one,
/// so all the usual methods are available.
#[derive(Debug, Clone, PartialEq, Eq, Allocative)]
pub struct CanonicalBTreeSet<T>(BTreeSet<T>);

impl<T> Default for CanonicalBTreeSet<T> {
    fn default() -> Self {
        Self(BTreeSet::new())
    }
}

impl<T> std::ops::Deref for CanonicalBTreeSet<T> {
    type Target = BTreeSet<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for CanonicalBTreeSet<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> From<BTreeSet<T>> for CanonicalBTreeSet<T> {
    fn from(set: BTreeSet<T>) -> Self {
        Self(set)
    }
}

impl<T> From<CanonicalBTreeSet<T>> for BTreeSet<T> {
    fn from(set: CanonicalBTreeSet<T>) -> Self {
        set.0
    }
}

impl<T: Ord> FromIterator<T> for CanonicalBTreeSet<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self(BTreeSet::from_iter(iter))
    }
}

impl<T> IntoIterator for CanonicalBTreeSet<T> {
    type Item = T;
    type IntoIter = std::collections::btree_set::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a CanonicalBTreeSet<T> {
    type Item = &'a T;
    type IntoIter = std::collections::btree_set::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T> Serialize for CanonicalBTreeSet<T>
where
    T: Serialize,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Serialize as a `BTreeMap<T, ()>`: going through `serialize_map` lets BCS sort the
        // elements canonically by their serialized bytes, as required in key position.
        serializer.collect_map(self.0.iter().map(|element| (element, ())))
    }
}

impl<'de, T> Deserialize<'de> for CanonicalBTreeSet<T>
where
    T: Deserialize<'de> + Ord,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let map = BTreeMap::<T, ()>::deserialize(deserializer)?;
        Ok(Self(map.into_keys().collect()))
    }
}

impl<T> async_graphql::OutputType for CanonicalBTreeSet<T>
where
    BTreeSet<T>: async_graphql::OutputType,
{
    fn type_name() -> std::borrow::Cow<'static, str> {
        <BTreeSet<T> as async_graphql::OutputType>::type_name()
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        <BTreeSet<T> as async_graphql::OutputType>::create_type_info(registry)
    }

    async fn resolve(
        &self,
        ctx: &async_graphql::ContextSelectionSet<'_>,
        field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
    ) -> async_graphql::ServerResult<async_graphql::Value> {
        self.0.resolve(ctx, field).await
    }
}

/// A non-negative amount of native tokens.
///
/// This is a fixed-point fraction, with [`Amount::DECIMAL_PLACES`] digits after the point.
/// [`Amount::ONE`] is one whole token, divisible into `10.pow(Amount::DECIMAL_PLACES)` parts.
pub type Amount = TokenAmount<NativeToken>;

#[derive(Serialize, Deserialize)]
#[serde(rename = "Amount")]
struct AmountString(String);

#[derive(Serialize, Deserialize)]
#[serde(rename = "Amount")]
struct AmountU128(u128);

impl<T> From<TokenAmount<T>> for U256 {
    fn from(amount: TokenAmount<T>) -> U256 {
        U256::from(amount.to_inner())
    }
}

impl<T: Token> From<TokenAmount<T>> for f64 {
    /// Returns the amount as a floating-point number of whole tokens. This is
    /// lossy for large or high-precision amounts; intended for telemetry, not
    /// for arithmetic.
    fn from(amount: TokenAmount<T>) -> f64 {
        amount.to_inner() as f64 / TokenAmount::<T>::one().to_inner() as f64
    }
}

impl<T> TryFrom<U256> for TokenAmount<T> {
    type Error = ArithmeticError;
    fn try_from(value: U256) -> Result<Self, Self::Error> {
        let value: u128 = value.try_into().map_err(|_| ArithmeticError::Overflow)?;
        Ok(Self::new(value))
    }
}

/// A non-negative amount of tokens with fixed (yet configurable) precision.
///
/// The type is "branded" by a type `T` implementing [`Token`]
///
/// The standard traits are implemented by hand rather than derived: the wrapper's behaviour
/// never depends on `T` (its only fields are a `u128` and a `PhantomData<T>`), so deriving them
/// would add spurious `T: Trait` bounds and force every `Token` marker to implement them too.
#[derive(WitType, WitLoad, WitStore)]
pub struct TokenAmount<T> {
    inner: u128,
    // `fn() -> T` rather than `T` so the wrapper is `Send + Sync` regardless of `T` (it never
    // actually holds a `T`), as required by the GraphQL `InputType`/`OutputType` impls.
    // `#[witty(skip)]` keeps the zero-sized marker out of the WIT interface (a `unit` record
    // field is not valid WIT); it is reconstructed via `Default` on load.
    #[witty(skip)]
    tag: std::marker::PhantomData<fn() -> T>,
}

// Hand-written (with a `T: Token` bound) rather than derived, because `proptest::Arbitrary`
// requires `Debug`, which is only implemented when `T: Token`.
#[cfg(all(with_testing, not(target_arch = "wasm32")))]
impl<T: Token + 'static> proptest::arbitrary::Arbitrary for TokenAmount<T> {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<Self>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        use proptest::strategy::Strategy as _;

        proptest::arbitrary::any::<u128>()
            .prop_map(Self::new)
            .boxed()
    }
}

impl<T> Clone for TokenAmount<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for TokenAmount<T> {}

impl<T> PartialEq for TokenAmount<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T> Eq for TokenAmount<T> {}

impl<T> PartialOrd for TokenAmount<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for TokenAmount<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl<T> Hash for TokenAmount<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl<T> Default for TokenAmount<T> {
    fn default() -> Self {
        Self::ZERO
    }
}

impl<T: Token> fmt::Debug for TokenAmount<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(T::NAME).field(&self.inner).finish()
    }
}

/// The specification of a token for [`TokenAmount`].
pub trait Token {
    /// The name of the token.
    const NAME: &'static str;

    /// Whether [`Display`] and [`FromStr`] use the decimal fixed-point form. When `false`, the
    /// inner `u128` value is used directly.
    const DECIMAL_DISPLAY: bool = true;

    /// The precision given as a number of decimals.
    fn decimals() -> u8;

    /// Returns `10.pow(exponent)`, panicking if the result does not fit in a `u128`.
    fn pow10(exponent: u8) -> u128 {
        10u128
            .checked_pow(exponent as u32)
            .expect("token precision is too high to represent as a fixed-point u128")
    }
}

/// The native token.
pub struct NativeToken;

impl Token for NativeToken {
    const NAME: &str = "Amount";

    fn decimals() -> u8 {
        Amount::DECIMAL_PLACES
    }
}

impl Amount {
    /// The base-10 exponent representing how much a token can be divided.
    pub const DECIMAL_PLACES: u8 = 18;

    /// One token.
    pub const ONE: Self = Self::new(10u128.pow(Self::DECIMAL_PLACES as u32));

    /// Returns the number of attotokens.
    pub const fn to_attos(self) -> u128 {
        self.to_inner()
    }
}

impl<T: Token> Allocative for TokenAmount<T> {
    fn visit<'a, 'b: 'a>(&self, visitor: &'a mut Visitor<'b>) {
        visitor.visit_simple_sized::<Self>();
    }
}

impl<T: Token> Display for TokenAmount<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !T::DECIMAL_DISPLAY {
            return Display::fmt(&self.inner, f);
        }
        // Print the wrapped integer, padded with zeros to cover a digit before the decimal point.
        let places = T::decimals() as usize;
        let min_digits = places + 1;
        let decimals = format!("{:0min_digits$}", self.inner);
        let integer_part = &decimals[..(decimals.len() - places)];
        let fractional_part = decimals[(decimals.len() - places)..].trim_end_matches('0');

        // For now, we never trim non-zero digits so we don't lose any precision.
        let precision = f.precision().unwrap_or(0).max(fractional_part.len());
        let sign = if f.sign_plus() && self.inner > 0 {
            "+"
        } else {
            ""
        };
        // A token with fractional places always shows the point (e.g. `1.`), matching the
        // convention that its output round-trips through `FromStr`. A zero-decimal token is an
        // integer, so the point only appears when a precision was explicitly requested.
        let point = if places > 0 || precision > 0 { "." } else { "" };
        // The amount of padding: desired width minus sign, point and number of digits.
        let pad_width = f.width().map_or(0, |w| {
            w.saturating_sub(precision)
                .saturating_sub(sign.len() + integer_part.len() + point.len())
        });
        let left_pad = match f.align() {
            None | Some(fmt::Alignment::Right) => pad_width,
            Some(fmt::Alignment::Center) => pad_width / 2,
            Some(fmt::Alignment::Left) => 0,
        };

        for _ in 0..left_pad {
            write!(f, "{}", f.fill())?;
        }
        write!(
            f,
            "{sign}{integer_part}{point}{fractional_part:0<precision$}"
        )?;
        for _ in left_pad..pad_width {
            write!(f, "{}", f.fill())?;
        }
        Ok(())
    }
}

impl<T: Token> FromStr for TokenAmount<T> {
    type Err = ParseAmountError;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        if !T::DECIMAL_DISPLAY {
            let inner = src
                .trim()
                .parse::<u128>()
                .map_err(|_| ParseAmountError::Parse)?;
            return Ok(Self::new(inner));
        }
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
                '.' => decimals = Some(T::decimals()),
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
            .checked_mul(T::pow10(decimals.unwrap_or_else(T::decimals)))
            .ok_or(ParseAmountError::TooHigh)?;
        Ok(Self::new(result))
    }
}

impl<T: Token> Serialize for TokenAmount<T> {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Override the container names below.
        let serializer = SerializeNameAdapter::new(serializer, T::NAME);
        if serializer.is_human_readable() {
            AmountString(self.to_string()).serialize(serializer)
        } else {
            AmountU128(self.inner).serialize(serializer)
        }
    }
}

impl<'de, T: Token> Deserialize<'de> for TokenAmount<T> {
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Override the container names below.
        let deserializer = DeserializeNameAdapter::new(deserializer, T::NAME);
        if deserializer.is_human_readable() {
            let AmountString(s) = AmountString::deserialize(deserializer)?;
            s.parse().map_err(serde::de::Error::custom)
        } else {
            Ok(Self::new(AmountU128::deserialize(deserializer)?.0))
        }
    }
}

// A GraphQL scalar named after the token, using the same string representation as the serde
// human-readable format (i.e. `Display`/`FromStr`). Implemented by hand rather than via
// `doc_scalar!` because the type is generic and the scalar name is `T::NAME`.
fn token_amount_scalar_meta<T: Token>() -> async_graphql::registry::MetaType {
    async_graphql::registry::MetaType::Scalar {
        name: T::NAME.to_owned(),
        description: Some("A non-negative amount of tokens.".to_string()),
        is_valid: Some(std::sync::Arc::new(|value| {
            <TokenAmount<T> as async_graphql::ScalarType>::is_valid(value)
        })),
        visible: None,
        inaccessible: false,
        tags: Default::default(),
        specified_by_url: None,
        directive_invocations: Default::default(),
        requires_scopes: Default::default(),
    }
}

impl<T: Token> async_graphql::ScalarType for TokenAmount<T> {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        Ok(async_graphql::from_value(value)?)
    }

    fn to_value(&self) -> async_graphql::Value {
        async_graphql::to_value(self).unwrap_or(async_graphql::Value::Null)
    }
}

impl<T: Token> async_graphql::InputType for TokenAmount<T> {
    type RawValueType = Self;

    fn type_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(T::NAME)
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        registry.create_input_type::<Self, _>(async_graphql::registry::MetaTypeId::Scalar, |_| {
            token_amount_scalar_meta::<T>()
        })
    }

    fn parse(value: Option<async_graphql::Value>) -> async_graphql::InputValueResult<Self> {
        <Self as async_graphql::ScalarType>::parse(value.unwrap_or_default())
    }

    fn to_value(&self) -> async_graphql::Value {
        <Self as async_graphql::ScalarType>::to_value(self)
    }

    fn as_raw_value(&self) -> Option<&Self::RawValueType> {
        Some(self)
    }
}

impl<T: Token> async_graphql::OutputType for TokenAmount<T> {
    fn type_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(T::NAME)
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        registry.create_output_type::<Self, _>(async_graphql::registry::MetaTypeId::Scalar, |_| {
            token_amount_scalar_meta::<T>()
        })
    }

    async fn resolve(
        &self,
        _: &async_graphql::ContextSelectionSet<'_>,
        _field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
    ) -> async_graphql::ServerResult<async_graphql::Value> {
        Ok(async_graphql::ScalarType::to_value(self))
    }
}

impl<'a, T: Token> iter::Sum<&'a TokenAmount<T>> for TokenAmount<T> {
    fn sum<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
        iter.fold(Self::ZERO, |a, b| a.saturating_add(*b))
    }
}

impl<T> From<TokenAmount<T>> for u128 {
    fn from(value: TokenAmount<T>) -> Self {
        value.inner
    }
}

// A `U128` is an explicit raw value, so converting to and from a `TokenAmount` is a deliberate
// crossing of the wire/typed boundary and available outside of tests (unlike the bare `u128`
// conversions below).
impl<T> From<TokenAmount<T>> for U128 {
    fn from(value: TokenAmount<T>) -> Self {
        U128(value.inner)
    }
}

impl<T> From<U128> for TokenAmount<T> {
    fn from(value: U128) -> Self {
        Self::new(value.0)
    }
}

// Cannot directly create values for a wrapped type, except for testing.
#[cfg(with_testing)]
impl<T> From<u128> for TokenAmount<T> {
    fn from(value: u128) -> Self {
        Self::new(value)
    }
}

#[cfg(with_testing)]
impl<T> ops::Add for TokenAmount<T> {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self::new(self.inner + other.inner)
    }
}

#[cfg(with_testing)]
impl<T> ops::Sub for TokenAmount<T> {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self::new(self.inner - other.inner)
    }
}

#[cfg(with_testing)]
impl<T> ops::Mul<u128> for TokenAmount<T> {
    type Output = Self;

    fn mul(self, other: u128) -> Self {
        Self::new(self.inner * other)
    }
}

impl<T> TokenAmount<T> {
    const fn new(inner: u128) -> Self {
        Self {
            inner,
            tag: std::marker::PhantomData,
        }
    }

    /// Zero tokens.
    pub const ZERO: Self = Self::new(0);

    /// The maximum representable amount.
    pub const MAX: Self = Self::new(u128::MAX);

    /// Returns the inner value.
    pub const fn to_inner(self) -> u128 {
        self.inner
    }

    /// Wraps a raw inner value, given in the token's smallest unit. Inverse of [`to_inner`].
    ///
    /// [`to_inner`]: Self::to_inner
    pub const fn from_inner(inner: u128) -> Self {
        Self::new(inner)
    }

    /// Returns whether this amount is 0.
    pub fn is_zero(&self) -> bool {
        self.inner == 0
    }

    /// Divides this by the other amount. If the other is 0, it returns `u128::MAX`.
    pub fn saturating_ratio(self, other: Self) -> u128 {
        self.inner.checked_div(other.inner).unwrap_or(u128::MAX)
    }

    /// Helper function to obtain the 64 most significant bits of the inner value.
    pub const fn upper_half(self) -> u64 {
        (self.inner >> 64) as u64
    }

    /// Helper function to obtain the 64 least significant bits of the inner value.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "intentional: returns the low 64 bits"
    )]
    pub const fn lower_half(self) -> u64 {
        self.inner as u64
    }
}

impl<T: Token> TokenAmount<T> {
    /// The precision in decimals.
    pub fn decimals() -> u8 {
        T::decimals()
    }

    /// One token.
    pub fn one() -> Self {
        TokenAmount::new(T::pow10(T::decimals()))
    }

    /// Checked addition.
    pub fn try_add(self, other: Self) -> Result<Self, ArithmeticError> {
        let val = self
            .inner
            .checked_add(other.inner)
            .ok_or(ArithmeticError::Overflow)?;
        Ok(Self::new(val))
    }

    /// Checked increment.
    pub fn try_add_one(self) -> Result<Self, ArithmeticError> {
        let val = self.inner.checked_add(1).ok_or(ArithmeticError::Overflow)?;
        Ok(Self::new(val))
    }

    /// Saturating addition.
    pub const fn saturating_add(self, other: Self) -> Self {
        let val = self.inner.saturating_add(other.inner);
        Self::new(val)
    }

    /// Checked subtraction.
    pub fn try_sub(self, other: Self) -> Result<Self, ArithmeticError> {
        let val = self
            .inner
            .checked_sub(other.inner)
            .ok_or(ArithmeticError::Underflow)?;
        Ok(Self::new(val))
    }

    /// Checked decrement.
    pub fn try_sub_one(self) -> Result<Self, ArithmeticError> {
        let val = self
            .inner
            .checked_sub(1)
            .ok_or(ArithmeticError::Underflow)?;
        Ok(Self::new(val))
    }

    /// Saturating subtraction.
    pub const fn saturating_sub(self, other: Self) -> Self {
        let val = self.inner.saturating_sub(other.inner);
        Self::new(val)
    }

    /// Returns the absolute difference between `self` and `other`.
    pub fn abs_diff(self, other: Self) -> Self {
        Self::new(self.inner.abs_diff(other.inner))
    }

    /// Returns the midpoint of `self` and `other`, rounded down.
    pub const fn midpoint(self, other: Self) -> Self {
        Self::new(self.inner.midpoint(other.inner))
    }

    /// Checked in-place addition.
    pub fn try_add_assign(&mut self, other: Self) -> Result<(), ArithmeticError> {
        self.inner = self
            .inner
            .checked_add(other.inner)
            .ok_or(ArithmeticError::Overflow)?;
        Ok(())
    }

    /// Checked in-place increment.
    pub fn try_add_assign_one(&mut self) -> Result<(), ArithmeticError> {
        self.inner = self.inner.checked_add(1).ok_or(ArithmeticError::Overflow)?;
        Ok(())
    }

    /// Saturating in-place addition.
    pub const fn saturating_add_assign(&mut self, other: Self) {
        self.inner = self.inner.saturating_add(other.inner);
    }

    /// Checked in-place subtraction.
    pub fn try_sub_assign(&mut self, other: Self) -> Result<(), ArithmeticError> {
        self.inner = self
            .inner
            .checked_sub(other.inner)
            .ok_or(ArithmeticError::Underflow)?;
        Ok(())
    }

    /// Saturating division.
    pub fn saturating_div(&self, other: u128) -> Self {
        Self::new(self.inner.checked_div(other).unwrap_or(u128::MAX))
    }

    /// Saturating multiplication.
    pub const fn saturating_mul(&self, other: u128) -> Self {
        Self::new(self.inner.saturating_mul(other))
    }

    /// Checked multiplication.
    pub fn try_mul(self, other: u128) -> Result<Self, ArithmeticError> {
        let val = self
            .inner
            .checked_mul(other)
            .ok_or(ArithmeticError::Overflow)?;
        Ok(Self::new(val))
    }

    /// Checked in-place multiplication.
    pub fn try_mul_assign(&mut self, other: u128) -> Result<(), ArithmeticError> {
        self.inner = self
            .inner
            .checked_mul(other)
            .ok_or(ArithmeticError::Overflow)?;
        Ok(())
    }

    /// Returns a `TokenAmount` for `amount` in units of `10.pow(-unit_decimals)` tokens,
    /// saturating on overflow and truncating any sub-unit remainder towards zero.
    fn from_subunits(amount: u128, unit_decimals: u8) -> Self {
        let decimals = T::decimals();
        if decimals >= unit_decimals {
            Self::new(T::pow10(decimals - unit_decimals)).saturating_mul(amount)
        } else {
            Self::new(amount / T::pow10(unit_decimals - decimals))
        }
    }

    /// Returns a `TokenAmount` corresponding to that many tokens, or [`TokenAmount::MAX`] if saturated.
    pub fn from_tokens(tokens: u128) -> Self {
        Self::from_subunits(tokens, 0)
    }

    /// Returns a `TokenAmount` corresponding to that many millitokens, or [`TokenAmount::MAX`] if saturated.
    pub fn from_millis(millitokens: u128) -> Self {
        Self::from_subunits(millitokens, 3)
    }

    /// Returns a `TokenAmount` corresponding to that many microtokens, or [`TokenAmount::MAX`] if saturated.
    pub fn from_micros(microtokens: u128) -> Self {
        Self::from_subunits(microtokens, 6)
    }

    /// Returns a `TokenAmount` corresponding to that many nanotokens, or [`TokenAmount::MAX`] if saturated.
    pub fn from_nanos(nanotokens: u128) -> Self {
        Self::from_subunits(nanotokens, 9)
    }

    /// Returns a `TokenAmount` corresponding to that many attotokens, or [`TokenAmount::MAX`] if saturated.
    pub fn from_attos(attotokens: u128) -> Self {
        Self::from_subunits(attotokens, 18)
    }
}

/// A `u128` newtype that serializes as a decimal string in human-readable
/// formats (JSON / GraphQL) and as a bare `u128` in binary (BCS).
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Hash,
    derive_more::Display,
    derive_more::Deref,
    derive_more::DerefMut,
    derive_more::FromStr,
)]
pub struct U128(pub u128);

impl Serialize for U128 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.0.to_string())
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for U128 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            s.parse().map(U128).map_err(serde::de::Error::custom)
        } else {
            u128::deserialize(deserializer).map(U128)
        }
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
    Allocative,
)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
pub struct BlockHeight(pub u64);

/// An identifier for successive attempts to decide a value in a consensus protocol.
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
    Allocative,
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
    Allocative,
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

    /// Returns the given [`Duration`] as a [`TimeDelta`], saturating at the maximum on overflow.
    pub fn from_duration(duration: Duration) -> Self {
        TimeDelta(u64::try_from(duration.as_micros()).unwrap_or(u64::MAX))
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
    Allocative,
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

impl FromStr for Timestamp {
    type Err = chrono::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let naive = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))?;
        let micros = naive
            .and_utc()
            .timestamp_micros()
            .try_into()
            .unwrap_or(u64::MAX);
        Ok(Timestamp(micros))
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

            /// Returns the absolute difference between `self` and `other`.
            pub fn abs_diff(self, other: Self) -> Self {
                Self(self.0.abs_diff(other.0))
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

            /// Saturating division.
            pub fn saturating_div(&self, other: $wrapped) -> Self {
                Self(self.0.checked_div(other).unwrap_or($wrapped::MAX))
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

impl_wrapped_number!(U128, u128);
impl_wrapped_number!(BlockHeight, u64);
impl_wrapped_number!(TimeDelta, u64);

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
            Round::MultiLeader(r) => write!(f, "multi-leader round {r}"),
            Round::SingleLeader(r) => write!(f, "single-leader round {r}"),
            Round::Validator(r) => write!(f, "validator round {r}"),
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

/// What created a chain.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize, Allocative,
)]
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
    /// Returns the root chain number, if this is a root chain.
    pub fn root(&self) -> Option<u32> {
        match self {
            ChainOrigin::Root(i) => Some(*i),
            ChainOrigin::Child { .. } => None,
        }
    }
}

/// A number identifying the configuration of the chain (aka the committee).
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Allocative)]
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
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Allocative)]
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
#[derive(Eq, PartialEq, Clone, Hash, Debug, Serialize, Deserialize, Allocative)]
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
    Allocative,
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
    /// These applications are allowed to close the current chain using the system API.
    #[graphql(default)]
    #[debug(skip_if = Vec::is_empty)]
    pub close_chain: Vec<ApplicationId>,
    /// These applications are allowed to change the application permissions using the system API.
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
    #[cfg(with_testing)]
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
            .is_none_or(|app_ids| app_ids.contains(app_id))
    }

    /// Returns whether the given application can make HTTP requests.
    pub fn can_make_http_requests(&self, app_id: &ApplicationId) -> bool {
        self.make_http_requests
            .as_ref()
            .is_none_or(|app_ids| app_ids.contains(app_id))
    }
}

/// A record of a single oracle response.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Allocative)]
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
    Event(
        EventId,
        #[debug(with = "hex_debug")]
        #[serde(with = "serde_bytes")]
        Vec<u8>,
    ),
    /// An event exists.
    EventExists(EventId),
}

impl BcsHashable<'_> for OracleResponse {}

/// Description of a user application.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Hash, Serialize, WitType, WitLoad, WitStore)]
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
    pub fn load_from_file(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        let bytes = fs::read(path).map_err(|error| {
            std::io::Error::new(error.kind(), format!("{}: {error}", path.display()))
        })?;
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

        #[cfg(with_metrics)]
        metrics::BYTECODE_DECOMPRESSED_SIZE_BYTES
            .with_label_values(&[])
            .observe(bytes.len() as f64);

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

        #[cfg(with_metrics)]
        BYTECODE_DECOMPRESSED_SIZE_BYTES
            .with_label_values(&[])
            .observe(bytes.len() as f64);

        Ok(Bytecode { bytes })
    }
}

impl BcsHashable<'_> for BlobContent {}

/// A blob of binary data.
#[serde_as]
#[derive(Hash, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Allocative)]
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

    /// Converts a `BlobContent` into `Vec<u8>` without cloning if possible.
    pub fn into_vec_or_clone(self) -> Vec<u8> {
        let bytes = Arc::unwrap_or_clone(self.bytes);
        bytes.into_vec()
    }

    /// Gets the `Arc<Box<[u8]>>` directly without cloning.
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

impl From<Arc<Blob>> for BlobContent {
    fn from(blob: Arc<Blob>) -> BlobContent {
        blob.content().clone()
    }
}

/// A blob of binary data, with its hash.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Allocative)]
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
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject, Allocative)]
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

/// Policies for automatically handling incoming messages.
#[derive(
    Clone, Debug, Default, serde::Serialize, serde::Deserialize, async_graphql::SimpleObject,
)]
pub struct MessagePolicy {
    /// The blanket policy applied to all messages.
    pub blanket: BlanketMessagePolicy,
    /// A collection of chains which restrict the origin of messages to be
    /// accepted. `Option::None` means that messages from all chains are accepted. An empty
    /// `HashSet` denotes that messages from no chains are accepted.
    pub restrict_chain_ids_to: Option<HashSet<ChainId>>,
    /// A collection of chains whose incoming messages should be ignored.
    pub ignore_chain_ids: HashSet<ChainId>,
    /// A collection of applications: If `Some`, only bundles with at least one message by any
    /// of these applications will be accepted.
    pub reject_message_bundles_without_application_ids: Option<HashSet<GenericApplicationId>>,
    /// A collection of applications: If `Some`, only bundles all of whose messages are by these
    /// applications will be accepted.
    pub reject_message_bundles_with_other_application_ids: Option<HashSet<GenericApplicationId>>,
    /// A collection of applications: If `Some`, only event streams from those
    /// applications will be processed.
    pub process_events_from_application_ids: Option<HashSet<GenericApplicationId>>,
    /// A collection of applications whose messages must never be rejected. Bundles whose
    /// messages are all from one of these applications bypass the other rejection rules
    /// (except `restrict_chain_ids_to`), and on execution failure they are discarded for
    /// later retry instead of being rejected. A bundle that contains any message from an
    /// application not on this list can be rejected. An empty set disables this feature.
    pub never_reject_application_ids: HashSet<GenericApplicationId>,
}

/// A blanket policy to apply to all messages by default.
#[derive(
    Default,
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    async_graphql::Enum,
)]
#[cfg_attr(web, derive(tsify::Tsify), tsify(from_wasm_abi, into_wasm_abi))]
#[cfg_attr(any(web, not(target_arch = "wasm32")), derive(clap::ValueEnum))]
pub enum BlanketMessagePolicy {
    /// Automatically accept all incoming messages. Reject them only if execution fails.
    #[default]
    Accept,
    /// Automatically reject tracked messages, ignore or skip untracked messages, but accept
    /// protected ones.
    Reject,
    /// Don't include any messages in blocks, and don't make any decision whether to accept or
    /// reject.
    Ignore,
}

impl MessagePolicy {
    /// Returns `true` if the blanket policy is to ignore messages.
    #[instrument(level = "trace", skip(self))]
    pub fn is_ignore(&self) -> bool {
        matches!(self.blanket, BlanketMessagePolicy::Ignore)
    }

    /// Returns `true` if the blanket policy is to reject messages.
    #[instrument(level = "trace", skip(self))]
    pub fn is_reject(&self) -> bool {
        matches!(self.blanket, BlanketMessagePolicy::Reject)
    }

    /// Returns `true` if every message from `origin` would be unconditionally dropped:
    /// blanket policy is `Ignore`, the origin is in `ignore_chain_ids`, or
    /// `restrict_chain_ids_to` is `Some` and does not contain the origin.
    #[instrument(level = "trace", skip(self))]
    pub fn ignores_origin(&self, origin: &ChainId) -> bool {
        self.is_ignore()
            || self.ignore_chain_ids.contains(origin)
            || self
                .restrict_chain_ids_to
                .as_ref()
                .is_some_and(|set| !set.contains(origin))
    }
}

doc_scalar!(Bytecode, "A WebAssembly module's bytecode");
doc_scalar!(U128, "A 128-bit unsigned integer.");
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

    use crate::prometheus_util::{
        exponential_bucket_interval, exponential_bucket_latencies, register_histogram_vec,
    };

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

    pub static BYTECODE_DECOMPRESSED_SIZE_BYTES: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "wasm_bytecode_decompressed_size_bytes",
            "Decompressed size in bytes of WASM bytecodes stored on-chain",
            &[],
            exponential_bucket_interval(10_000.0, 100_000_000.0),
        )
    });
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use alloy_primitives::U256;

    use super::{Amount, ApplicationDescription, BlobContent};
    use crate::{
        crypto::CryptoHash,
        data_types::BlockHeight,
        identifiers::{BlobType, ChainId, ModuleId},
        vm::VmRuntime,
    };

    #[test]
    fn non_canonical_btree_map_serializes_like_vec() {
        use std::collections::BTreeMap;

        use super::NonCanonicalBTreeMap;

        // `256u32` is chosen so that its little-endian BCS bytes sort *before* `1u32`'s,
        // i.e. the canonical (serialized-byte) order differs from the numeric `Ord` order.
        let map = NonCanonicalBTreeMap::from(BTreeMap::from([
            (1u32, 10u8),
            (256u32, 20u8),
            (2u32, 30u8),
        ]));

        // It serializes as a plain `Vec<(K, V)>` in the map's `Ord` key order, with no canonical
        // re-sorting.
        let entries = map
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect::<Vec<(u32, u8)>>();
        assert_eq!(
            bcs::to_bytes(&map).unwrap(),
            bcs::to_bytes(&entries).unwrap()
        );

        // ... which differs from the canonical `BTreeMap` encoding that re-sorts by serialized key.
        let canonical = map
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect::<BTreeMap<u32, u8>>();
        assert_ne!(
            bcs::to_bytes(&map).unwrap(),
            bcs::to_bytes(&canonical).unwrap()
        );

        // It round-trips.
        let deserialized: NonCanonicalBTreeMap<u32, u8> =
            bcs::from_bytes(&bcs::to_bytes(&map).unwrap()).unwrap();
        assert_eq!(map, deserialized);
    }

    #[test]
    fn canonical_btree_set_serializes_like_map() {
        use std::collections::{BTreeMap, BTreeSet};

        use super::CanonicalBTreeSet;

        let set = CanonicalBTreeSet::from(BTreeSet::from([1u32, 256u32, 2u32]));

        // It serializes exactly like a `BTreeMap<T, ()>`, i.e. canonically sorted by serialized
        // bytes.
        let map = set.iter().map(|t| (*t, ())).collect::<BTreeMap<u32, ()>>();
        assert_eq!(bcs::to_bytes(&set).unwrap(), bcs::to_bytes(&map).unwrap());

        // That canonical order differs from a plain `BTreeSet`'s sequence encoding, which keeps
        // the numeric `Ord` order.
        let plain = set.iter().copied().collect::<BTreeSet<u32>>();
        assert_ne!(bcs::to_bytes(&set).unwrap(), bcs::to_bytes(&plain).unwrap());

        // It round-trips.
        let deserialized: CanonicalBTreeSet<u32> =
            bcs::from_bytes(&bcs::to_bytes(&set).unwrap()).unwrap();
        assert_eq!(set, deserialized);
    }

    #[test]
    fn display_amount() {
        assert_eq!("1.", Amount::ONE.to_string());
        assert_eq!("1.", Amount::from_str("1.").unwrap().to_string());
        assert_eq!(
            Amount::from(10_000_000_000_000_000_000),
            Amount::from_str("10").unwrap()
        );
        assert_eq!("10.", Amount::from(10_000_000_000_000_000_000).to_string());
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
        // `Debug` uses the token name (`NativeToken::NAME`), matching the old derived output.
        assert_eq!("Amount(1000000000000000000)", format!("{:?}", Amount::ONE));
    }

    #[test]
    fn display_token_amount() {
        use super::{Token, TokenAmount};

        // A token with two decimal places.
        struct Cents;
        impl Token for Cents {
            const NAME: &'static str = "Cents";

            fn decimals() -> u8 {
                2
            }
        }

        // A zero-decimal (integer) token.
        struct Units;
        impl Token for Units {
            const NAME: &'static str = "Units";

            fn decimals() -> u8 {
                0
            }
        }

        // `Debug` is tagged with the token's name.
        assert_eq!("Cents(100)", format!("{:?}", TokenAmount::<Cents>::one()));

        // A fractional token keeps the trailing point on whole numbers, like `Amount`.
        assert_eq!("1.", TokenAmount::<Cents>::one().to_string());
        assert_eq!(
            "1.23",
            TokenAmount::<Cents>::from_str("1.23").unwrap().to_string()
        );
        assert_eq!("1.00", format!("{:.2}", TokenAmount::<Cents>::one()));

        // A zero-decimal token prints as a bare integer, with no dangling point.
        assert_eq!(
            "5",
            TokenAmount::<Units>::from_str("5").unwrap().to_string()
        );
        assert_eq!("0", TokenAmount::<Units>::ZERO.to_string());
        // ... unless a precision is explicitly requested.
        assert_eq!(
            "5.00",
            format!("{:.2}", TokenAmount::<Units>::from_str("5").unwrap())
        );

        // `MAX` is the largest inner value, independent of the precision.
        assert_eq!(u128::MAX, TokenAmount::<Cents>::MAX.to_inner());
        assert_eq!(u128::MAX, TokenAmount::<Units>::MAX.to_inner());
    }

    #[test]
    fn token_amount_from_subunits() {
        use super::{Token, TokenAmount};

        // A token with two decimal places, coarser than milli/micro/etc.
        struct Cents;
        impl Token for Cents {
            const NAME: &'static str = "Cents";

            fn decimals() -> u8 {
                2
            }
        }

        // Units at or coarser than the token's precision scale up exactly.
        assert_eq!("1.", TokenAmount::<Cents>::from_tokens(1).to_string());
        assert_eq!("2.5", TokenAmount::<Cents>::from_millis(2500).to_string());

        // Finer units than the token can represent are truncated towards zero rather than
        // panicking: 1500 millitokens = 1.5 tokens, but 1 millitoken (0.001) rounds down to 0.
        assert_eq!("1.5", TokenAmount::<Cents>::from_millis(1500).to_string());
        assert_eq!("0.", TokenAmount::<Cents>::from_millis(1).to_string());
        assert_eq!(
            "0.12",
            TokenAmount::<Cents>::from_micros(123_456).to_string()
        );
        assert_eq!("0.", TokenAmount::<Cents>::from_micros(9999).to_string());

        // Scaling up saturates instead of overflowing.
        assert_eq!(
            TokenAmount::<Cents>::MAX,
            TokenAmount::<Cents>::from_tokens(u128::MAX)
        );
    }

    #[test]
    #[should_panic(expected = "token precision is too high")]
    fn token_amount_precision_too_high_panics() {
        use super::{Token, TokenAmount};

        // 39 decimals cannot be represented: `10.pow(39)` overflows a `u128`.
        struct TooPrecise;
        impl Token for TooPrecise {
            const NAME: &'static str = "TooPrecise";

            fn decimals() -> u8 {
                39
            }
        }

        // Panics loudly rather than silently wrapping in release builds.
        TokenAmount::<TooPrecise>::one();
    }

    #[test]
    fn token_amount_raw_u128_display() {
        use super::{Token, TokenAmount};

        // A token that displays and parses as its raw inner `u128`.
        struct Raw;
        impl Token for Raw {
            const NAME: &'static str = "Raw";
            const DECIMAL_DISPLAY: bool = false;

            fn decimals() -> u8 {
                18
            }
        }

        // Display writes the inner value verbatim, ignoring `decimals()`; no decimal point.
        assert_eq!(
            "12345",
            TokenAmount::<Raw>::from_str("12345").unwrap().to_string()
        );
        assert_eq!("0", TokenAmount::<Raw>::ZERO.to_string());

        // FromStr parses a plain integer straight into the inner value, round-tripping.
        let parsed = TokenAmount::<Raw>::from_str("999").unwrap();
        assert_eq!(999, parsed.to_inner());
        assert_eq!("999", parsed.to_string());
        assert!(TokenAmount::<Raw>::from_str("1.5").is_err());
    }

    #[test]
    fn token_amount_json_and_graphql_match_display() {
        use async_graphql::ScalarType;

        use super::{Token, TokenAmount};

        // A decimal token and a raw-u128 token.
        struct Cents;
        impl Token for Cents {
            const NAME: &'static str = "Cents";

            fn decimals() -> u8 {
                2
            }
        }
        struct Raw;
        impl Token for Raw {
            const NAME: &'static str = "Raw";
            const DECIMAL_DISPLAY: bool = false;

            fn decimals() -> u8 {
                2
            }
        }

        let cents = TokenAmount::<Cents>::from_str("1.5").unwrap();
        let raw = TokenAmount::<Raw>::from_str("12345").unwrap();

        // JSON serializes as the `Display` string and round-trips through `FromStr`.
        assert_eq!("\"1.5\"", serde_json::to_string(&cents).unwrap());
        assert_eq!("\"12345\"", serde_json::to_string(&raw).unwrap());
        assert_eq!(cents, serde_json::from_str("\"1.5\"").unwrap());
        assert_eq!(raw, serde_json::from_str("\"12345\"").unwrap());

        // The GraphQL scalar uses the same string form for both output and input.
        assert_eq!(
            async_graphql::Value::String(cents.to_string()),
            cents.to_value()
        );
        assert_eq!(
            async_graphql::Value::String(raw.to_string()),
            raw.to_value()
        );
        assert_eq!(
            cents,
            TokenAmount::<Cents>::parse(cents.to_value()).unwrap()
        );
        assert_eq!(raw, TokenAmount::<Raw>::parse(raw.to_value()).unwrap());
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

    #[test]
    fn test_conversion_amount_u256() {
        let value_amount = Amount::from_tokens(15656565652209004332);
        let value_u256: U256 = value_amount.into();
        let value_amount_rev = Amount::try_from(value_u256).expect("Failed conversion");
        assert_eq!(value_amount, value_amount_rev);
    }

    #[test]
    fn token_amount_generic_conversions() {
        use super::{Token, TokenAmount};

        // A branded (non-native) token exercises the generalized conversions.
        struct Cents;
        impl Token for Cents {
            const NAME: &'static str = "Cents";

            fn decimals() -> u8 {
                2
            }
        }

        // `upper_half`/`lower_half` operate on the raw inner value.
        let amount = TokenAmount::<Cents>::from_str("1.50").unwrap();
        assert_eq!(150, amount.to_inner());
        assert_eq!(0, amount.upper_half());
        assert_eq!(150, amount.lower_half());

        // `U256` conversions round-trip for any token.
        let value_u256: U256 = amount.into();
        assert_eq!(amount, TokenAmount::<Cents>::try_from(value_u256).unwrap());

        // `f64` uses the token's own precision: 150 base units = 1.5 whole tokens.
        assert_eq!(1.5, f64::from(amount));
    }

    /// `linera-explorer` running on `wasm32` does not have access to the
    /// strongly-typed `ApplicationDescription`: the GraphQL client substitutes
    /// it for `serde_json::Value`. The explorer therefore fetches the module ID
    /// for an application by indexing into the JSON object as
    /// `description["module_id"]`. This test pins that field name and the
    /// hex-string shape of the serialized `ModuleId` so a future rename or
    /// representation change immediately breaks here instead of silently in the
    /// browser.
    #[test]
    fn application_description_serializes_module_id_as_hex_string() {
        let module_id = ModuleId::new(
            CryptoHash::test_hash("contract-bytecode"),
            CryptoHash::test_hash("service-bytecode"),
            VmRuntime::Wasm,
        );
        let description = ApplicationDescription {
            module_id,
            creator_chain_id: ChainId(CryptoHash::test_hash("chain")),
            block_height: BlockHeight(0),
            application_index: 0,
            parameters: Vec::new(),
            required_application_ids: Vec::new(),
        };

        let value = serde_json::to_value(&description).unwrap();
        let module_id_value = value
            .get("module_id")
            .expect("`module_id` is the field name the explorer indexes into");
        let hex = module_id_value
            .as_str()
            .expect("`module_id` must serialize as a hex string in human-readable form");
        let roundtrip: ModuleId =
            serde_json::from_value(serde_json::Value::String(hex.to_owned())).unwrap();
        assert_eq!(roundtrip, module_id);
    }
}
