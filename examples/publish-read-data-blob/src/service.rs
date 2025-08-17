// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use linera_sdk::{
    bcs,
    linera_base_types::{BlobContent, CryptoHash, WithServiceAbi},
    views::View,
    DataBlobHash, Service, ServiceRuntime,
};
use publish_read_data_blob::{Operation, PublishReadDataBlobAbi, ServiceQuery};

use self::state::PublishReadDataBlobState;

pub struct PublishReadDataBlobService {
    state: Arc<PublishReadDataBlobState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(PublishReadDataBlobService);

impl WithServiceAbi for PublishReadDataBlobService {
    type Abi = PublishReadDataBlobAbi;
}

impl Service for PublishReadDataBlobService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = PublishReadDataBlobState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        PublishReadDataBlobService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, query: ServiceQuery) -> Vec<u8> {
        match query {
            ServiceQuery::GetHash => match self.state.hash.get() {
                Some(hash) => bcs::to_bytes(&hash).unwrap(),
                None => panic!("No Hash stored"),
            },
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
