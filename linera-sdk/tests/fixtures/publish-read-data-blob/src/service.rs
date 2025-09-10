// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use linera_sdk::{
    linera_base_types::{BlobContent, CryptoHash, DataBlobHash, WithServiceAbi},
    Service, ServiceRuntime,
};
use publish_read_data_blob::{Operation, PublishReadDataBlobAbi, ServiceQuery};

pub struct PublishReadDataBlobService {
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(PublishReadDataBlobService);

impl WithServiceAbi for PublishReadDataBlobService {
    type Abi = PublishReadDataBlobAbi;
}

impl Service for PublishReadDataBlobService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        PublishReadDataBlobService {
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, query: ServiceQuery) -> Vec<u8> {
        match query {
            ServiceQuery::PublishDataBlob(data) => {
                self.runtime
                    .schedule_operation(&Operation::CreateDataBlob(data));
                Vec::new()
            }
            ServiceQuery::ReadDataBlob(hash, expected_data) => {
                self.runtime
                    .schedule_operation(&Operation::ReadDataBlob(hash, expected_data));
                Vec::new()
            }
            ServiceQuery::PublishAndCreateOneOperation(data) => {
                self.runtime
                    .schedule_operation(&Operation::CreateAndReadDataBlob(data));
                Vec::new()
            }
            ServiceQuery::PublishAndCreateTwoOperations(data) => {
                // First operation: create the blob
                self.runtime
                    .schedule_operation(&Operation::CreateDataBlob(data.clone()));

                // Compute the blob_id from the data
                let content = BlobContent::new_data(data.clone());
                let hash = DataBlobHash(CryptoHash::new(&content));

                // Second operation: read the blob with verification
                self.runtime
                    .schedule_operation(&Operation::ReadDataBlob(hash, data));
                Vec::new()
            }
        }
    }
}
