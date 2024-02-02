// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types to interface with the system API available to application services.

#[cfg(not(any(test, feature = "test")))]
mod private;
#[cfg(any(test, feature = "test"))]
pub mod private;

pub(crate) use self::private::{current_application_parameters, load_view, query_application};
use super::service_system_api as wit;
use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::{ApplicationId, ChainId, Owner},
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

/// Retrieves the current system time, i.e. the timestamp of the latest block in this chain.
pub fn current_system_time() -> Timestamp {
    wit::read_system_timestamp().into()
}

/// Requests the host to log a message.
///
/// Useful for debugging locally, but may be ignored by validators.
pub fn log(message: &fmt::Arguments<'_>, level: log::Level) {
    wit::log(&message.to_string(), level.into());
}
