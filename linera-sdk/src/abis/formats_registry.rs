// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI of the Formats Registry application.
//!
//! See `examples/formats-registry` for the contract and service implementation.

use async_graphql::{Request, Response};
use serde::{Deserialize, Serialize};

use crate::linera_base_types::{ContractAbi, ModuleId, ServiceAbi};

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
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Operation {
    /// Publish `value` as a data blob and bind it to `module_id`. A given
    /// `module_id` may only be written once.
    Write { module_id: ModuleId, value: Vec<u8> },
}
