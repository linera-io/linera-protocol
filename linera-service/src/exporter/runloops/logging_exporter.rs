// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::IntoFuture, io::Write, path::Path};

use linera_chain::types::CertificateValue;
use tokio::select;

use crate::storage::ExporterStorage;

/// A logging exporter that writes logs to a file.
///
/// This exporter does not track any state or process data; it simply logs messages to a specified file.
/// It will export events as they occur, never exporting past ones,
/// which can be useful for debugging and monitoring purposes.
pub(crate) struct LoggingExporter {
    file: std::fs::File,
}

impl LoggingExporter {
    /// Creates a new `LoggingExporter` that logs to the specified file.
    pub fn new(log_file: &Path) -> Self {
        let file = std::fs::File::create(log_file).expect("Failed to create log file");
        LoggingExporter { file }
    }

    pub(crate) async fn run_with_shutdown<S, F: IntoFuture<Output = ()>>(
        self,
        shutdown_signal: F,
        storage: ExporterStorage<S>,
    ) -> anyhow::Result<()>
    where
        S: linera_storage::Storage + Clone + Send + Sync + 'static,
    {
        let shutdown_signal_future = shutdown_signal.into_future();
        let mut pinned_shutdown_signal = Box::pin(shutdown_signal_future);

        select! {
            _ = &mut pinned_shutdown_signal => {
                tracing::info!("logging exporter shutdown signal received, exiting.");
            }

            _ = self.start_logger(storage) => {

            }
        }
        Ok(())
    }

    async fn start_logger<S>(mut self, storage: ExporterStorage<S>) -> anyhow::Result<()>
    where
        S: linera_storage::Storage + Clone + Send + Sync + 'static,
    {
        let mut canonical_chain_height = storage.get_latest_index();
        tracing::info!(
            "starting logging exporter at height {}",
            canonical_chain_height
        );

        loop {
            if let Ok((block, blobs)) = storage.get_block_with_blobs(canonical_chain_height).await {
                let inner = block.inner();
                writeln!(
                    self.file,
                    "Block ID: {}, Chain: {}, Height: {}, State Hash: {}, Authenticated Signer: {}",
                    inner.hash(),
                    inner.chain_id(),
                    inner.height(),
                    inner.block().header.state_hash,
                    inner
                        .block()
                        .header
                        .authenticated_signer
                        .map(|signer| signer.to_string())
                        .unwrap_or_else(|| "N/A".into()),
                )?;
                for blob in blobs {
                    writeln!(self.file, "\tBlob ID: {}", blob.id(),)?;
                }

                canonical_chain_height += 1;
            } else {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
}
