// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Formats Registry application.

This application maps a [`ModuleId`] to the bytes of a `Formats` value
(typically the BCS serialization of the module's `serde_reflection`
registry). The contract publishes the value as an immutable
[`linera_sdk::linera_base_types::DataBlob`] and stores the resulting
[`DataBlobHash`] in a [`linera_sdk::views::MapView`]. The service exposes
a `get(module_id)` query that returns the bytes by reading the data blob.
*/

use async_graphql::{Request, Response};
use linera_sdk::linera_base_types::{ContractAbi, ModuleId, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct FormatsRegistryAbi;

impl ContractAbi for FormatsRegistryAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for FormatsRegistryAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// Operations accepted by the formats-registry contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    /// Publish `value` as a data blob and bind it to `module_id`.
    /// A given `module_id` may only be written once.
    Write { module_id: ModuleId, value: Vec<u8> },
}
