// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime types to interface with the host executing the contract.

use super::wit_system_api as wit;
use linera_base::{
    data_types::BlockHeight,
    identifiers::{ApplicationId, ChainId, Owner},
};

/// The common runtime to interface with the host executing the contract.
///
/// It automatically caches read-only values received from the host.
#[derive(Clone, Debug, Default)]
pub struct ContractRuntime {
    application_id: Option<ApplicationId>,
    chain_id: Option<ChainId>,
    authenticated_signer: Option<Option<Owner>>,
    block_height: Option<BlockHeight>,
}

impl ContractRuntime {
    /// Returns the ID of the current application.
    pub fn application_id(&mut self) -> ApplicationId {
        *self
            .application_id
            .get_or_insert_with(|| wit::application_id().into())
    }

    /// Returns the ID of the current chain.
    pub fn chain_id(&mut self) -> ChainId {
        *self.chain_id.get_or_insert_with(|| wit::chain_id().into())
    }

    /// Returns the authenticated signer for this execution, if there is one.
    pub fn authenticated_signer(&mut self) -> Option<Owner> {
        *self
            .authenticated_signer
            .get_or_insert_with(|| wit::authenticated_signer().map(Owner::from))
    }

    /// Returns the height of the current block that is executing.
    pub fn block_height(&mut self) -> BlockHeight {
        *self
            .block_height
            .get_or_insert_with(|| wit::block_height().into())
    }
}
