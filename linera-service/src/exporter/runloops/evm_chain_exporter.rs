// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::IntoFuture, sync::atomic::Ordering};

use linera_base::identifiers::BlobType;
use linera_bridge::evm_client::EvmLightClient;
use linera_execution::{system::AdminOperation, Operation, SystemOperation};
use linera_service::config::{Destination, DestinationId};
use tokio::select;

use crate::storage::ExporterStorage;

/// An exporter that relays committee changes to a LightClient contract on an EVM chain.
///
/// Scans each canonical block for `CreateCommittee` operations and, when found,
/// calls `LightClient.addCommittee()` on the configured EVM endpoint.
pub(crate) struct EvmChainExporter {
    id: DestinationId,
    evm_client: EvmLightClient,
}

impl EvmChainExporter {
    pub fn new(id: DestinationId, destination: Destination) -> Self {
        let (endpoint, light_client_address, private_key) = match destination {
            Destination::EvmChain {
                endpoint,
                light_client_address,
                private_key,
            } => (endpoint, light_client_address, private_key),
            _ => panic!("EvmChainExporter requires an EvmChain destination"),
        };

        let contract_address = light_client_address
            .parse()
            .expect("invalid LightClient contract address");

        let evm_client = EvmLightClient::new(&endpoint, contract_address, &private_key)
            .expect("failed to create EVM light client");

        EvmChainExporter { id, evm_client }
    }

    pub(crate) async fn run_with_shutdown<S, F: IntoFuture<Output = ()>>(
        self,
        shutdown_signal: F,
        storage: ExporterStorage<S>,
    ) -> anyhow::Result<()>
    where
        S: linera_storage::Storage + Clone + Send + Sync + 'static,
    {
        let id = self.id.clone();
        let shutdown_signal_future = shutdown_signal.into_future();
        let mut pinned_shutdown_signal = Box::pin(shutdown_signal_future);

        select! {
            _ = &mut pinned_shutdown_signal => {
                tracing::info!(?id, "EVM chain exporter shutdown signal received, exiting.");
            }

            result = self.run_loop(storage) => {
                if let Err(e) = result {
                    tracing::error!(?id, error=?e, "EVM chain exporter failed");
                }
            }
        }
        Ok(())
    }

    async fn run_loop<S>(self, storage: ExporterStorage<S>) -> anyhow::Result<()>
    where
        S: linera_storage::Storage + Clone + Send + Sync + 'static,
    {
        let destination_state = storage.load_destination_state(&self.id);
        let mut destination_height = destination_state.load(Ordering::Acquire) as usize;
        tracing::info!(height = destination_height, "starting EVM chain exporter");

        loop {
            if let Ok((block_cert, blobs)) = storage.get_block_with_blobs(destination_height).await
            {
                let inner = block_cert.inner();

                // Scan for CreateCommittee operations
                let committee_blob_hash = inner.block().body.operations().find_map(|op| {
                    if let Operation::System(boxed) = op {
                        if let SystemOperation::Admin(AdminOperation::CreateCommittee {
                            blob_hash,
                            ..
                        }) = boxed.as_ref()
                        {
                            return Some(*blob_hash);
                        }
                    }
                    None
                });

                if let Some(blob_hash) = committee_blob_hash {
                    let blob_id =
                        linera_base::identifiers::BlobId::new(blob_hash, BlobType::Committee);

                    // Find the committee blob — it may be in this block's blobs
                    // (if PublishCommitteeBlob and CreateCommittee are in the same block)
                    // or it may have been published in an earlier block.
                    let committee_blob = match blobs.iter().find(|b| b.id() == blob_id) {
                        Some(blob) => Some(blob.clone()),
                        None => match storage.get_blob(blob_id).await {
                            Ok(blob) => Some(blob),
                            Err(e) => {
                                tracing::error!(
                                    height = destination_height,
                                    ?blob_id,
                                    error = ?e,
                                    "CreateCommittee blob not found in block or storage"
                                );
                                None
                            }
                        },
                    };

                    if let Some(blob) = committee_blob {
                        let committee_bytes = blob.bytes();
                        let certificate_bytes =
                            bcs::to_bytes(&*block_cert).expect("BCS serialization failed");

                        match self
                            .evm_client
                            .add_committee(&certificate_bytes, committee_bytes)
                            .await
                        {
                            Ok(tx_hash) => {
                                tracing::info!(
                                    %tx_hash,
                                    height = destination_height,
                                    "successfully relayed committee to EVM"
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    height = destination_height,
                                    error = ?e,
                                    "failed to relay committee to EVM"
                                );
                            }
                        }
                    }
                }

                // Always advance, even if no committee change
                destination_state.fetch_add(1, Ordering::Release);
                destination_height += 1;
            } else {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
}
