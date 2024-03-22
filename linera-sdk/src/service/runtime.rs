// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime types to interface with the host executing the service.

use super::service_system_api as wit;
use linera_base::{data_types::BlockHeight, identifiers::ChainId};

/// The runtime available during execution of a query.
#[derive(Clone, Debug, Default)]
pub struct ServiceRuntime {
}

impl ServiceRuntime {
    /// Returns the ID of the current chain.
    pub fn chain_id(&self) -> ChainId {
        wit::chain_id().into()
    }

    /// Returns the height of the next block that can be added to the current chain.
    pub fn next_block_height(&self) -> BlockHeight {
        wit::next_block_height().into()
    }

    /// TODO
    pub fn get_blob_from_url(&self, url: &str) -> Vec<u8> {
        wit::get_blob_from_url(url)
    }
}
