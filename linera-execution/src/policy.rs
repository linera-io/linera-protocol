// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains types related to fees and pricing.

use async_graphql::InputObject;
use linera_base::data_types::{Amount, ArithmeticError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A collection of costs associated with blocks in validators.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Serialize, Deserialize, InputObject)]
pub struct ResourceControlPolicy {
    /// The base price for each certificate, to compensate for the communication and signing
    /// overhead.
    pub certificate: Amount,
    /// The price per unit of fuel used when executing messages and operations for user applications.
    pub fuel: Amount,
    /// The cost to read data per operation
    pub storage_num_reads: Amount,
    /// The cost to read data per byte
    pub storage_bytes_read: Amount,
    /// The cost to store data per byte
    pub storage_bytes_written: Amount,
    /// The maximum data to read per block
    pub maximum_bytes_read_per_block: u64,
    /// The maximum data to write per block
    pub maximum_bytes_written_per_block: u64,
    /// The cost to store and send cross-chain messages, per byte.
    pub messages: Amount,
}

impl Default for ResourceControlPolicy {
    fn default() -> Self {
        ResourceControlPolicy {
            certificate: Amount::default(),
            fuel: Amount::default(),
            storage_num_reads: Amount::default(),
            storage_bytes_read: Amount::default(),
            storage_bytes_written: Amount::default(),
            maximum_bytes_read_per_block: u64::MAX / 2,
            maximum_bytes_written_per_block: u64::MAX / 2,
            messages: Amount::default(),
        }
    }
}

impl ResourceControlPolicy {
    pub fn certificate_price(&self) -> Amount {
        self.certificate
    }

    pub fn messages_price(&self, data: &impl Serialize) -> Result<Amount, PricingError> {
        let size =
            u128::try_from(bcs::serialized_size(data)?).map_err(|_| ArithmeticError::Overflow)?;
        Ok(self.messages.try_mul(size)?)
    }

    pub fn storage_num_reads_price(&self, size: &u64) -> Result<Amount, PricingError> {
        let size = *size as u128;
        Ok(self.storage_num_reads.try_mul(size)?)
    }

    pub fn storage_bytes_read_price(&self, size: &u64) -> Result<Amount, PricingError> {
        let size = *size as u128;
        Ok(self.storage_bytes_read.try_mul(size)?)
    }

    pub fn storage_bytes_written_price(&self, size: &u64) -> Result<Amount, PricingError> {
        let size = *size as u128;
        Ok(self.storage_bytes_written.try_mul(size)?)
    }

    pub fn storage_bytes_written_price_raw(
        &self,
        data: &impl Serialize,
    ) -> Result<Amount, PricingError> {
        let size =
            u128::try_from(bcs::serialized_size(data)?).map_err(|_| ArithmeticError::Overflow)?;
        Ok(self.storage_bytes_written.try_mul(size)?)
    }

    pub fn fuel_price(&self, fuel: u64) -> Result<Amount, PricingError> {
        Ok(self.fuel.try_mul(u128::from(fuel))?)
    }

    /// Returns how much fuel can be paid with the given balance.
    pub fn remaining_fuel(&self, balance: Amount) -> u64 {
        u64::try_from(balance.saturating_div(self.fuel)).unwrap_or(u64::MAX)
    }

    #[cfg(any(test, feature = "test"))]
    /// Creates a pricing with no cost for anything except fuel.
    ///
    /// This can be used in tests that need whole numbers in their chain balance and don't expect
    /// to execute any Wasm code.
    pub fn only_fuel() -> Self {
        ResourceControlPolicy {
            certificate: Amount::ZERO,
            fuel: Amount::from_atto(1_000_000_000_000),
            storage_num_reads: Amount::ZERO,
            storage_bytes_read: Amount::ZERO,
            storage_bytes_written: Amount::ZERO,
            maximum_bytes_read_per_block: u64::MAX / 2,
            maximum_bytes_written_per_block: u64::MAX / 2,
            messages: Amount::ZERO,
        }
    }

    #[cfg(any(test, feature = "test"))]
    /// Creates a pricing with no cost for anything except fuel, and 0.001 per certificate.
    ///
    /// This can be used in tests that don't expect to execute any Wasm code, and that keep track of
    /// how many certificates were created.
    pub fn fuel_and_certificate() -> Self {
        ResourceControlPolicy {
            certificate: Amount::from_milli(1),
            fuel: Amount::from_atto(1_000_000_000_000),
            storage_num_reads: Amount::ZERO,
            storage_bytes_read: Amount::ZERO,
            storage_bytes_written: Amount::ZERO,
            maximum_bytes_read_per_block: u64::MAX,
            maximum_bytes_written_per_block: u64::MAX,
            messages: Amount::ZERO,
        }
    }

    #[cfg(any(test, feature = "test"))]
    /// Creates a pricing where all categories have a small non-zero cost.
    pub fn all_categories() -> Self {
        ResourceControlPolicy {
            certificate: Amount::from_milli(1),
            fuel: Amount::from_atto(1_000_000_000),
            storage_num_reads: Amount::ZERO,
            storage_bytes_read: Amount::from_atto(100),
            storage_bytes_written: Amount::from_atto(1_000),
            maximum_bytes_read_per_block: u64::MAX,
            maximum_bytes_written_per_block: u64::MAX,
            messages: Amount::from_atto(1),
        }
    }
}

#[derive(Error, Debug)]
pub enum PricingError {
    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),
    #[error(transparent)]
    SerializationError(#[from] bcs::Error),
}
