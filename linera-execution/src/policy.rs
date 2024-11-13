// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains types related to fees and pricing.

use std::{
    cmp,
    fmt::{self, Display, Formatter},
    ops::{Deref, DerefMut},
    str::FromStr,
};

use async_graphql::InputObject;
use linera_base::{
    data_types::{Amount, ArithmeticError, Resources},
    doc_scalar,
};
use serde::{Deserialize, Serialize};

/// A collection of prices and limits associated with block execution.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Serialize, Deserialize, InputObject)]
pub struct ResourceControlPolicy {
    /// The base price for creating a new block.
    pub block: Amount,
    /// The price per unit of fuel (aka gas) for VM execution.
    pub fuel_unit: Amount,
    /// The price of one read operation.
    pub read_operation: Amount,
    /// The price of one write operation.
    pub write_operation: Amount,
    /// The price of reading a byte.
    pub byte_read: Amount,
    /// The price of writing a byte
    pub byte_written: Amount,
    /// The price of increasing storage by a byte.
    // TODO(#1536): This is not fully supported.
    pub byte_stored: Amount,
    /// The base price of adding an operation to a block.
    pub operation: Amount,
    /// The additional price for each byte in the argument of a user operation.
    pub operation_byte: Amount,
    /// The base price of sending a message from a block.
    pub message: Amount,
    /// The additional price for each byte in the argument of a user message.
    pub message_byte: Amount,

    // TODO(#1538): Cap the number of transactions per block and the total size of their
    // arguments.
    /// The maximum amount of fuel a block can consume.
    pub maximum_fuel_per_block: u64,
    /// The maximum size of an executed block. This includes the block proposal itself as well as
    /// the execution outcome.
    pub maximum_executed_block_size: u64,
    /// The maximum size of decompressed contract or service bytecode, in bytes.
    pub maximum_bytecode_size: u64,
    /// The maximum size of a blob.
    pub maximum_blob_size: u64,
    /// The maximum data to read per block
    pub maximum_bytes_read_per_block: u64,
    /// The maximum data to write per block
    pub maximum_bytes_written_per_block: u64,
}

impl Display for ResourceControlPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let ResourceControlPolicy {
            block,
            fuel_unit,
            read_operation,
            write_operation,
            byte_read,
            byte_written,
            byte_stored,
            operation,
            operation_byte,
            message,
            message_byte,
            maximum_fuel_per_block,
            maximum_executed_block_size,
            maximum_blob_size,
            maximum_bytecode_size,
            maximum_bytes_read_per_block,
            maximum_bytes_written_per_block,
        } = self;
        write!(
            f,
            "Resource control policy:\n\
            {block:.2} base cost per block\n\
            {fuel_unit:.2} cost per fuel unit\n\
            {read_operation:.2} cost per read operation\n\
            {write_operation:.2} cost per write operation\n\
            {byte_read:.2} cost per byte read\n\
            {byte_written:.2} cost per byte written\n\
            {byte_stored:.2} cost per byte stored\n\
            {operation:.2} per operation\n\
            {operation_byte:.2} per byte in the argument of an operation\n\
            {message:.2} per outgoing messages\n\
            {message_byte:.2} per byte in the argument of an outgoing messages\n\
            {maximum_fuel_per_block} maximum fuel per block\n\
            {maximum_executed_block_size} maximum size of an executed block\n\
            {maximum_blob_size} maximum size of a data blob, bytecode or other binary blob\n\
            {maximum_bytecode_size} maximum size of service and contract bytecode\n\
            {maximum_bytes_read_per_block} maximum number bytes read per block\n\
            {maximum_bytes_written_per_block} maximum number bytes written per block",
        )
    }
}

impl Default for ResourceControlPolicy {
    fn default() -> Self {
        Self {
            block: Amount::default(),
            fuel_unit: Amount::default(),
            read_operation: Amount::default(),
            write_operation: Amount::default(),
            byte_read: Amount::default(),
            byte_written: Amount::default(),
            byte_stored: Amount::default(),
            operation: Amount::default(),
            operation_byte: Amount::default(),
            message: Amount::default(),
            message_byte: Amount::default(),
            maximum_fuel_per_block: u64::MAX,
            maximum_executed_block_size: u64::MAX,
            maximum_blob_size: u64::MAX,
            maximum_bytecode_size: u64::MAX,
            maximum_bytes_read_per_block: u64::MAX,
            maximum_bytes_written_per_block: u64::MAX,
        }
    }
}

impl ResourceControlPolicy {
    pub fn block_price(&self) -> Amount {
        self.block
    }

    pub fn total_price(&self, resources: &Resources) -> Result<Amount, ArithmeticError> {
        let mut amount = Amount::ZERO;
        amount.try_add_assign(self.fuel_price(resources.fuel)?)?;
        amount.try_add_assign(self.read_operations_price(resources.read_operations)?)?;
        amount.try_add_assign(self.write_operations_price(resources.write_operations)?)?;
        amount.try_add_assign(self.bytes_read_price(resources.bytes_to_read as u64)?)?;
        amount.try_add_assign(self.bytes_written_price(resources.bytes_to_write as u64)?)?;
        amount.try_add_assign(self.message.try_mul(resources.messages as u128)?)?;
        amount.try_add_assign(self.message_bytes_price(resources.message_size as u64)?)?;
        amount.try_add_assign(self.bytes_stored_price(resources.storage_size_delta as u64)?)?;
        Ok(amount)
    }

    pub(crate) fn operation_bytes_price(&self, size: u64) -> Result<Amount, ArithmeticError> {
        self.operation_byte.try_mul(size as u128)
    }

    pub(crate) fn message_bytes_price(&self, size: u64) -> Result<Amount, ArithmeticError> {
        self.message_byte.try_mul(size as u128)
    }

    pub(crate) fn read_operations_price(&self, count: u32) -> Result<Amount, ArithmeticError> {
        self.read_operation.try_mul(count as u128)
    }

    pub(crate) fn write_operations_price(&self, count: u32) -> Result<Amount, ArithmeticError> {
        self.write_operation.try_mul(count as u128)
    }

    pub(crate) fn bytes_read_price(&self, count: u64) -> Result<Amount, ArithmeticError> {
        self.byte_read.try_mul(count as u128)
    }

    pub(crate) fn bytes_written_price(&self, count: u64) -> Result<Amount, ArithmeticError> {
        self.byte_written.try_mul(count as u128)
    }

    // TODO(#1536): This is not fully implemented.
    #[allow(dead_code)]
    pub(crate) fn bytes_stored_price(&self, count: u64) -> Result<Amount, ArithmeticError> {
        self.byte_stored.try_mul(count as u128)
    }

    pub(crate) fn fuel_price(&self, fuel: u64) -> Result<Amount, ArithmeticError> {
        self.fuel_unit.try_mul(u128::from(fuel))
    }

    /// Returns how much fuel can be paid with the given balance.
    pub(crate) fn remaining_fuel(&self, balance: Amount) -> u64 {
        u64::try_from(balance.saturating_div(self.fuel_unit)).unwrap_or(u64::MAX)
    }
}

impl ResourceControlPolicy {
    /// Creates a policy with no cost for anything except fuel.
    ///
    /// This can be used in tests that need whole numbers in their chain balance and don't expect
    /// to execute any Wasm code.
    pub fn only_fuel() -> Self {
        Self {
            fuel_unit: Amount::from_micros(1),
            ..Self::default()
        }
    }

    /// Creates a policy with no cost for anything except fuel, and 0.001 per block.
    ///
    /// This can be used in tests that don't expect to execute any Wasm code, and that keep track of
    /// how many blocks were created.
    pub fn fuel_and_block() -> Self {
        Self {
            block: Amount::from_millis(1),
            fuel_unit: Amount::from_micros(1),
            ..Self::default()
        }
    }

    /// Creates a policy where all categories have a small non-zero cost.
    pub fn all_categories() -> Self {
        Self {
            block: Amount::from_millis(1),
            fuel_unit: Amount::from_nanos(1),
            byte_read: Amount::from_attos(100),
            byte_written: Amount::from_attos(1_000),
            operation: Amount::from_attos(10),
            operation_byte: Amount::from_attos(1),
            message: Amount::from_attos(10),
            message_byte: Amount::from_attos(1),
            ..Self::default()
        }
    }

    /// Creates a policy that matches the Devnet.
    pub fn devnet() -> Self {
        Self {
            block: Amount::from_millis(1),
            fuel_unit: Amount::from_nanos(10),
            byte_read: Amount::from_nanos(10),
            byte_written: Amount::from_nanos(100),
            read_operation: Amount::from_micros(10),
            write_operation: Amount::from_micros(20),
            byte_stored: Amount::from_nanos(10),
            message_byte: Amount::from_nanos(100),
            operation_byte: Amount::from_nanos(10),
            operation: Amount::from_micros(10),
            message: Amount::from_micros(10),
            maximum_fuel_per_block: 100_000_000,
            maximum_executed_block_size: 1_000_000,
            maximum_blob_size: 1_000_000,
            maximum_bytecode_size: 10_000_000,
            maximum_bytes_read_per_block: 100_000_000,
            maximum_bytes_written_per_block: 10_000_000,
        }
    }
}

/// A wrapper type representing a resource limit.
///
/// The limit is serialized as a string in human-readable serialization formats in order to support
/// 64 bit integers in JSON without having to use floating point numbers.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceLimit(pub u64);

/// Private helper type to handle serialization of a [`ResourceLimit`] as a [`String`].
#[derive(Serialize, Deserialize)]
#[serde(rename = "ResourceLimit")]
struct ResourceLimitAsString(String);

/// Private helper type to handle serialization of a [`ResourceLimit`] as an [`u64`].
#[derive(Serialize, Deserialize)]
#[serde(rename = "ResourceLimit")]
struct ResourceLimitAsU64(u64);

impl Default for ResourceLimit {
    fn default() -> Self {
        ResourceLimit(u64::MAX)
    }
}

impl Deref for ResourceLimit {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ResourceLimit {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromStr for ResourceLimit {
    type Err = <u64 as FromStr>::Err;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(ResourceLimit(string.parse()?))
    }
}

impl Display for ResourceLimit {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl PartialEq<u64> for ResourceLimit {
    fn eq(&self, other: &u64) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<ResourceLimit> for u64 {
    fn eq(&self, other: &ResourceLimit) -> bool {
        self.eq(&other.0)
    }
}

impl PartialOrd<u64> for ResourceLimit {
    fn partial_cmp(&self, other: &u64) -> Option<cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl PartialOrd<ResourceLimit> for u64 {
    fn partial_cmp(&self, other: &ResourceLimit) -> Option<cmp::Ordering> {
        self.partial_cmp(&other.0)
    }
}

impl Serialize for ResourceLimit {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            ResourceLimitAsString(self.0.to_string()).serialize(serializer)
        } else {
            ResourceLimitAsU64(self.0).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for ResourceLimit {
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let limit = if deserializer.is_human_readable() {
            let ResourceLimitAsString(string) = ResourceLimitAsString::deserialize(deserializer)?;
            string.parse().map_err(serde::de::Error::custom)?
        } else {
            ResourceLimitAsU64::deserialize(deserializer)?.0
        };

        Ok(ResourceLimit(limit))
    }
}

doc_scalar!(
    ResourceLimit,
    "A maximal value for the usage of a resource."
);
