// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types to interface with the system API available to application contracts.

#[cfg(not(any(test, feature = "test")))]
mod private;
#[cfg(any(test, feature = "test"))]
pub mod private;

pub(crate) use self::private::{
    call_application, call_session, current_application_parameters, load_view, store_view,
};
use super::contract_system_api as wit;
use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::{Account, ApplicationId, ChainId, Owner},
    ownership::ChainOwnership,
};
use std::fmt;

/// Retrieves the current chain ID.
pub fn current_chain_id() -> ChainId {
    ChainId(wit::chain_id().into())
}

/// Retrieves the current application ID.
pub fn current_application_id() -> ApplicationId {
    wit::application_id().into()
}

/// Retrieves the current chain balance.
pub fn current_chain_balance() -> Amount {
    wit::read_chain_balance().into()
}

/// Retrieves the current balance for a given owner.
pub fn current_owner_balance(owner: Owner) -> Amount {
    wit::read_owner_balance(owner.into()).into()
}

/// Transfers amount from source to destination
pub fn transfer(source: Option<Owner>, destination: Account, amount: Amount) {
    wit::transfer(
        source.map(|source| source.into()),
        destination.into(),
        amount.into(),
    )
}

/// Claims amount from source to destination
pub fn claim(source: Account, destination: Account, amount: Amount) {
    wit::claim(source.into(), destination.into(), amount.into())
}

/// Retrieves the owner configuration for the current chain.
pub fn chain_ownership() -> ChainOwnership {
    wit::chain_ownership().into()
}

/// Retrieves the current system time, i.e. the timestamp of the block in which this is called.
pub fn current_system_time() -> Timestamp {
    wit::read_system_timestamp().into()
}

/// Requests the host to log a message.
///
/// Useful for debugging locally, but may be ignored by validators.
pub fn log(message: &fmt::Arguments<'_>, level: log::Level) {
    wit::log(&message.to_string(), level.into());
}
