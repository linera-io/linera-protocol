// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs::OpenOptions, future::IntoFuture, io::Write, path::Path, sync::atomic::Ordering};

use linera_chain::types::CertificateValue;
use linera_service::config::DestinationId;
use tokio::select;

use crate::storage::ExporterStorage;

/// A logging exporter that writes logs to a file.
///
/// This exporter does not track any state or process data; it simply logs messages to a specified file.
/// It will export events as they occur, never exporting past ones,
/// which can be useful for debugging and monitoring purposes.
pub(crate) struct LoggingExporter {
    id: DestinationId,
    file: std::fs::File,
}

impl LoggingExporter {
    /// Creates a new `LoggingExporter` that logs to the specified file.
    pub fn new(id: DestinationId) -> Self {
        let log_file = Path::new(id.address());
        // Don't truncate the file to preserve previous logs
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(log_file)
            .expect("Failed to create log file");
        LoggingExporter { id, file }
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
                tracing::info!(?id, "logging exporter shutdown signal received, exiting.");
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
        let destination_state = storage.load_destination_state(&self.id);
        let mut destination_height = destination_state.load(Ordering::Acquire) as usize;
        tracing::info!("starting logging exporter at height {}", destination_height);

        loop {
            if let Ok((block, blobs)) = storage.get_block_with_blobs(destination_height).await {
                let inner = block.inner();
                writeln!(
                    self.file,
                    "Block ID: {}, Chain: {}, Height: {}, State Hash: {}, Authenticated Owner: {}",
                    inner.hash(),
                    inner.chain_id(),
                    inner.height(),
                    inner.block().header.state_hash,
                    inner
                        .block()
                        .header
                        .authenticated_owner
                        .map_or_else(|| "N/A".into(), |signer| signer.to_string()),
                )?;
                for blob in blobs {
                    writeln!(self.file, "\tBlob ID: {}", blob.id(),)?;
                }

                destination_state.fetch_add(1, Ordering::Release);
                destination_height += 1;
            } else {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
}
