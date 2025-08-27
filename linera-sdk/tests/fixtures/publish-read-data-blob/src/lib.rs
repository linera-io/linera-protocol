// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Publish-Read Data Blob Example Application */

use linera_sdk::linera_base_types::{ContractAbi, DataBlobHash, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct PublishReadDataBlobAbi;

#[derive(Debug, Deserialize, Serialize)]
pub enum Operation {
    CreateDataBlob(Vec<u8>),
    ReadDataBlob(DataBlobHash, Vec<u8>),
    CreateAndReadDataBlob(Vec<u8>),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum ServiceQuery {
    PublishDataBlob(Vec<u8>),
    ReadDataBlob(DataBlobHash, Vec<u8>),
    PublishAndCreateOneOperation(Vec<u8>),
    PublishAndCreateTwoOperations(Vec<u8>),
}

impl ContractAbi for PublishReadDataBlobAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for PublishReadDataBlobAbi {
    type Query = ServiceQuery;
    type QueryResponse = Vec<u8>;
}
