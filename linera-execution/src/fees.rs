// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains types related to fees and pricing.

use async_graphql::InputObject;
use linera_base::data_types::{Amount, ArithmeticError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A list of prices for different cost categories that each block causes for the validators.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Default, Serialize, Deserialize, InputObject)]
pub struct Pricing {
    /// The base price for each certificate, to compensate for the communication and signing
    /// overhead.
    pub certificate: Amount,
    /// The price per unit of fuel used when executing effects and operations for user applications.
    pub fuel: Amount,
    /// The cost to store the new certificate, per byte.
    pub storage: Amount,
    /// The cost to send cross-chain messages, per byte.
    pub messages: Amount,
}

impl Pricing {
    #[cfg(any(test, feature = "test"))]
    pub fn make_simple() -> Self {
        Pricing {
            certificate: Amount::ZERO,
            fuel: Amount::ZERO,
            storage: Amount::ZERO,
            messages: Amount::ZERO,
        }
    }

    pub fn certificate_price(&self) -> Amount {
        self.certificate
    }

    pub fn storage_and_messages_price(
        &self,
        data: &impl Serialize,
    ) -> Result<Amount, PricingError> {
        let size =
            u128::try_from(bcs::serialized_size(data)?).map_err(|_| ArithmeticError::Overflow)?;
        let storage_and_messages = self.storage.saturating_add(self.messages);
        Ok(storage_and_messages.saturating_mul(size))
    }

    pub fn storage_price(&self, data: &impl Serialize) -> Result<Amount, PricingError> {
        let size =
            u128::try_from(bcs::serialized_size(data)?).map_err(|_| ArithmeticError::Overflow)?;
        Ok(self.storage.saturating_mul(size))
    }

    pub fn fuel_price(&self, fuel: u64) -> Amount {
        self.fuel.saturating_mul(u128::from(fuel))
    }
}

#[derive(Error, Debug)]
pub enum PricingError {
    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),
    #[error(transparent)]
    SerializationError(#[from] bcs::Error),
}
