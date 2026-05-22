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
mod tip_lag;

pub use config::Benchmark;

use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use linera_client::client_context::ClientContext;
use linera_core::node::{ValidatorNode as _, ValidatorNodeProvider as _};

use self::report::{Candidate, Layers, Metadata, Observer, OutputSpec, Report, Writer};

impl Benchmark {
    pub async fn run(
        &self,
        context: &mut ClientContext<
            impl linera_core::Environment<ValidatorNode = linera_rpc::Client>,
        >,
    ) -> Result<()> {
        // Validate output specs up front so a typo fails fast, before any work.
        let output_specs = OutputSpec::parse_all(&self.output)?;

        let started_at = Utc::now();
        let node = context.make_node_provider().make_node(&self.address)?;

        // L1 preflight also yields version/network info for the report metadata.
        // When skipped, fetch those two cheap fields best-effort anyway.
        let (preflight, version_info, network_description) = if !self.skip_preflight {
            let outcome = preflight::run(&node).await;
            if self.abort_on_preflight_fail
                && outcome.report.status == report::PreflightStatus::Fail
            {
                anyhow::bail!(
                    "preflight failed for {}: {:?}",
                    self.address,
                    outcome.report.errors
                );
            }
            (
                Some(outcome.report),
                outcome.version_info,
                outcome.network_description,
            )
        } else {
            let version_info = node.get_version_info().await.ok().map(|v| format!("{v:?}"));
            let network_description = node
                .get_network_description()
                .await
                .ok()
                .and_then(|nd| serde_json::to_value(nd).ok());
            (None, version_info, network_description)
        };

        let read_baseline = if !self.skip_read_baseline {
            Some(read_latency::run_baseline(&node, &self.chain, self.baseline_requests).await)
        } else {
            None
        };

        let read_stress = if !self.skip_read_stress {
            Some(
                read_latency::run_stress(
                    &node,
                    &self.chain,
                    &self.stress_levels,
                    Duration::from_secs(self.stress_duration_secs),
                )
                .await,
            )
        } else {
            None
        };

        let bulk_download = if !self.skip_bulk_download {
            Some(
                bulk_download::run(
                    &node,
                    &self.chain,
                    self.bulk_batch_size,
                    &self.bulk_concurrency,
                    &self.bulk_height_range,
                )
                .await?,
            )
        } else {
            None
        };

        let tip_lag = if !self.skip_tip_lag {
            Some(
                tip_lag::run(
                    &node,
                    context,
                    &self.chain,
                    self.tip_lag_samples,
                    Duration::from_secs(self.tip_lag_interval_secs),
                )
                .await?,
            )
        } else {
            None
        };

        let partial_sync = if self.deep {
            let deep_chain = self.deep_chain.unwrap_or(self.chain[0]);
            Some(partial_sync::run(&node, context, deep_chain, self.deep_blocks).await?)
        } else {
            None
        };

        let ended_at = Utc::now();
        let report = Report {
            metadata: Metadata {
                tool_version: env!("CARGO_PKG_VERSION").to_string(),
                candidate: Candidate {
                    address: self.address.clone(),
                    public_key: self.public_key.map(|k| k.to_string()),
                    version_info,
                    network_description,
                },
                observer: Observer {
                    location: self.observer_location.clone(),
                    hostname: std::env::var("HOSTNAME").unwrap_or_default(),
                    started_at: started_at.to_rfc3339(),
                    ended_at: Some(ended_at.to_rfc3339()),
                    duration_secs: Some((ended_at - started_at).num_seconds().max(0) as u64),
                },
                config: serde_json::to_value(self)?,
                chains_tested: self.chain.iter().map(|c| c.to_string()).collect(),
            },
            layers: Layers {
                preflight,
                read_baseline,
                read_stress,
                bulk_download,
                tip_lag,
                partial_sync,
            },
        };

        Writer::new(output_specs).emit(&report)?;
        Ok(())
    }
}
