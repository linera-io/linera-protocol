// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains types related to fees and pricing.

use async_graphql::InputObject;
use linera_base::data_types::{Amount, ArithmeticError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A collection of costs associated with blocks in validators.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Default, Serialize, Deserialize, InputObject)]
pub struct Pricing {
    /// The base price for each certificate, to compensate for the communication and signing
    /// overhead.
    pub certificate: Amount,
    /// The price per unit of fuel used when executing effects and operations for user applications.
    pub fuel: Amount,
    /// The cost to store a block's operations and incoming messages, per byte.
    pub storage: Amount,
    /// The cost to store and send cross-chain messages, per byte.
    pub messages: Amount,
}

impl Pricing {
    pub fn certificate_price(&self) -> Amount {
        self.certificate
    }

    pub fn messages_price(&self, data: &impl Serialize) -> Result<Amount, PricingError> {
        let size =
            u128::try_from(bcs::serialized_size(data)?).map_err(|_| ArithmeticError::Overflow)?;
        Ok(self.messages.saturating_mul(size))
    }

    pub fn storage_price(&self, data: &impl Serialize) -> Result<Amount, PricingError> {
        let size =
            u128::try_from(bcs::serialized_size(data)?).map_err(|_| ArithmeticError::Overflow)?;
        Ok(self.storage.saturating_mul(size))
    }

    pub fn fuel_price(&self, fuel: u64) -> Amount {
        self.fuel.saturating_mul(u128::from(fuel))
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
        Pricing {
            certificate: Amount::ZERO,
            fuel: Amount::from_atto(1_000_000_000_000),
            storage: Amount::ZERO,
            messages: Amount::ZERO,
        }
    }

    #[cfg(any(test, feature = "test"))]
    /// Creates a pricing with no cost for anything except fuel, and 0.001 per certificate.
    ///
    /// This can be used in tests that don't expect to execute any Wasm code, and that keep track of
    /// how many certificates were created.
    pub fn fuel_and_certificate() -> Self {
        Pricing {
            certificate: Amount::from_milli(1),
            fuel: Amount::from_atto(1_000_000_000_000),
            storage: Amount::ZERO,
            messages: Amount::ZERO,
        }
    }

    #[cfg(any(test, feature = "test"))]
    /// Creates a pricing where all categories have a small non-zero cost.
    pub fn all_categories() -> Self {
        Pricing {
            certificate: Amount::from_milli(1),
            fuel: Amount::from_atto(1_000_000_000),
            storage: Amount::from_atto(1_000),
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
