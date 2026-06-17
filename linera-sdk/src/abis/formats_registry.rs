// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI of the Formats Registry application.
//!
//! See `examples/formats-registry` for the contract and service implementation.

use async_graphql::{Request, Response};
use serde::{Deserialize, Serialize};

use crate::linera_base_types::{AccountOwner, ContractAbi, ModuleId, ServiceAbi};

/// The ABI of the formats-registry application.
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
///
/// This is the minimal interface relied upon by `linera
/// publish-module-with-formats`. A concrete registry implementation (see
/// `examples/formats-registry`) may define its own, richer `Operation` enum with
/// extra admin commands; as long as `Write` stays the first variant with the same
/// fields, its BCS encoding remains compatible with the operation produced here.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[allow(missing_docs)]
pub enum Operation {
    /// Publish `value` as a data blob and bind it to `module_id`, on behalf of
    /// `owner`. A given `module_id` may only be written once.
    Write {
        owner: AccountOwner,
        module_id: ModuleId,
        value: Vec<u8>,
    },
}
