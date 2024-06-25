// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains types related to fees and pricing.

use async_graphql::InputObject;
use linera_base::data_types::{Amount, ArithmeticError, Resources};
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
    /// The maximum data to read per block
    pub maximum_bytes_read_per_block: u64,
    /// The maximum data to write per block
    pub maximum_bytes_written_per_block: u64,
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
            maximum_bytes_read_per_block: 100_000_000,
            maximum_bytes_written_per_block: 10_000_000,
        }
    }
}
