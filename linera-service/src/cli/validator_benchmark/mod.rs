// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Multi-layer pre-onboarding benchmark for a candidate validator.
//!
//! Tracking: linera-io/linera-infra#1198.

mod bulk_download;
mod config;
mod latency;
mod partial_sync;
mod preflight;
mod progress;
mod read_latency;
mod report;
mod rpc;
mod tip_lag;

use std::{io::IsTerminal as _, time::Duration};

use anyhow::Result;
use chrono::Utc;
pub use config::Benchmark;
use linera_base::identifiers::ChainId;
use linera_client::client_context::ClientContext;
use linera_core::{
    data_types::ChainInfoQuery,
    node::{ValidatorNode, ValidatorNodeProvider as _},
};

use self::{
    progress::Progress,
    report::{Candidate, Layers, Metadata, Observer, OutputSpec, Report, Writer},
};

impl Benchmark {
    pub async fn run(
        &self,
        context: &mut ClientContext<
            impl linera_core::Environment<ValidatorNode = linera_rpc::Client>,
        >,
    ) -> Result<()> {
        // Validate output specs up front so a typo fails fast, before any work.
        let output_specs = OutputSpec::parse_all(&self.output)?;

        let progress = Progress::new(!self.no_progress && std::io::stderr().is_terminal());
        let rpc_timeout = Duration::from_secs(self.rpc_timeout_secs);

        let started_at = Utc::now();
        let node = context.make_node_provider().make_node(&self.address)?;
        let writer = Writer::new(output_specs);

        // Build the report up front and flush file targets after each layer, so
        // an interrupted run still leaves the completed layers on disk.
        let mut report = Report {
            metadata: Metadata {
                tool_version: env!("CARGO_PKG_VERSION").to_string(),
                candidate: Candidate {
                    address: self.address.clone(),
                    public_key: self.public_key.map(|k| k.to_string()),
                    version_info: None,
                    network_description: None,
                },
                observer: Observer {
                    location: self.observer_location.clone(),
                    hostname: std::env::var("HOSTNAME").unwrap_or_default(),
                    started_at: started_at.to_rfc3339(),
                    ended_at: None,
                    duration_secs: None,
                },
                config: serde_json::to_value(self)?,
                chains_tested: self.chain.iter().map(|c| c.to_string()).collect(),
                complete: false,
            },
            layers: Layers::default(),
        };

        // L1 preflight also yields version/network info for the report metadata.
        // When skipped, fetch those two cheap fields best-effort anyway.
        if !self.skip_preflight {
            let outcome = preflight::run(&node, rpc_timeout, &progress).await;
            if self.abort_on_preflight_fail
                && outcome.report.status == report::PreflightStatus::Fail
            {
                progress.clear();
                anyhow::bail!(
                    "preflight failed for {}: {:?}",
                    self.address,
                    outcome.report.errors
                );
            }
            report.metadata.candidate.version_info = outcome.version_info;
            report.metadata.candidate.network_description = outcome.network_description;
            report.layers.preflight = Some(outcome.report);
        } else {
            report.metadata.candidate.version_info =
                rpc::timed(rpc_timeout, node.get_version_info())
                    .await
                    .ok()
                    .map(|v| format!("{v:?}"));
            report.metadata.candidate.network_description =
                rpc::timed(rpc_timeout, node.get_network_description())
                    .await
                    .ok()
                    .and_then(|nd| serde_json::to_value(nd).ok());
        }
        writer.write_files(&report)?;

        // A not-yet-committee candidate may hold few or no blocks. Seed first when
        // --deep so the read layers below exercise a candidate that actually has
        // the data, and warn about any chain it does not hold and will not seed.
        let deep_chain = self.deep.then(|| self.deep_chain.unwrap_or(self.chain[0]));
        warn_unheld_chains(&node, &self.chain, deep_chain, rpc_timeout).await;
        if let Some(deep_chain) = deep_chain {
            report.layers.partial_sync = Some(
                Box::pin(partial_sync::run(
                    &node,
                    context,
                    deep_chain,
                    self.deep_blocks,
                    rpc_timeout,
                    &progress,
                ))
                .await?,
            );
            writer.write_files(&report)?;
        }

        // Layer futures are large; box them at the await site (clippy::large_futures).
        if !self.skip_read_baseline {
            report.layers.read_baseline = Some(
                Box::pin(read_latency::run_baseline(
                    &node,
                    &self.chain,
                    self.baseline_requests,
                    rpc_timeout,
                    &progress,
                ))
                .await,
            );
            writer.write_files(&report)?;
        }

        if !self.skip_read_stress {
            report.layers.read_stress = Some(
                Box::pin(read_latency::run_stress(
                    &node,
                    &self.chain,
                    &self.stress_levels,
                    Duration::from_secs(self.stress_duration_secs),
                    rpc_timeout,
                    &progress,
                ))
                .await,
            );
            writer.write_files(&report)?;
        }

        if !self.skip_bulk_download {
            report.layers.bulk_download = Some(
                Box::pin(bulk_download::run(
                    &node,
                    &self.chain,
                    self.bulk_batch_size,
                    &self.bulk_concurrency,
                    &self.bulk_height_range,
                    rpc_timeout,
                    &progress,
                ))
                .await?,
            );
            writer.write_files(&report)?;
        }

        if !self.skip_tip_lag {
            report.layers.tip_lag = Some(
                Box::pin(tip_lag::run(
                    &node,
                    context,
                    &self.chain,
                    self.tip_lag_samples,
                    Duration::from_secs(self.tip_lag_interval_secs),
                    rpc_timeout,
                    &progress,
                ))
                .await?,
            );
            writer.write_files(&report)?;
        }

        let ended_at = Utc::now();
        report.metadata.observer.ended_at = Some(ended_at.to_rfc3339());
        report.metadata.observer.duration_secs =
            Some(u64::try_from((ended_at - started_at).num_seconds()).unwrap_or(0));
        report.metadata.complete = true;
        progress.clear();
        writer.emit(&report)?;
        Ok(())
    }
}

/// Warn about chains the candidate does not hold (read layers would be shallow),
/// excluding one that `--deep` is about to seed. A chain with tip 0 or an
/// unreachable lookup is treated as not held.
async fn warn_unheld_chains(
    node: &impl ValidatorNode,
    chains: &[ChainId],
    seeded: Option<ChainId>,
    rpc_timeout: Duration,
) {
    let mut unheld = Vec::new();
    for &chain in chains {
        if Some(chain) == seeded {
            continue;
        }
        let held = rpc::timed(
            rpc_timeout,
            node.handle_chain_info_query(ChainInfoQuery::new(chain)),
        )
        .await
        .is_ok_and(|response| response.info.next_block_height.0 > 0);
        if !held {
            unheld.push(chain.to_string());
        }
    }
    if !unheld.is_empty() {
        tracing::warn!(
            "candidate does not hold chain(s) [{}]; read layers (L2-L4) will be shallow. \
             Pre-sync them (`linera validator sync`) or pass `--deep` to seed blocks first.",
            unheld.join(", ")
        );
    }
}
