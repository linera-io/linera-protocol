// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use hdrhistogram::Histogram;
use linera_core::client::TimingType;
use tokio::{sync::mpsc, task, time};
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct TimingConfig {
    pub enabled: bool,
    pub report_interval_secs: u64,
}

#[cfg(not(web))]
impl Default for TimingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            report_interval_secs: 5,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientMetricsError {
    #[error("Failed to create histogram: {0}")]
    HistogramCreationError(#[from] hdrhistogram::CreationError),
    #[error("Failed to record histogram: {0}")]
    HistogramRecordError(#[from] hdrhistogram::RecordError),
}

pub struct ExecuteBlockTimingsHistograms {
    pub submit_block_proposal_histogram: Histogram<u64>,
    pub update_validators_histogram: Histogram<u64>,
}

impl ExecuteBlockTimingsHistograms {
    pub fn new() -> Result<Self, ClientMetricsError> {
        Ok(Self {
            submit_block_proposal_histogram: Histogram::<u64>::new(2)?,
            update_validators_histogram: Histogram::<u64>::new(2)?,
        })
    }
}

pub struct ExecuteOperationsTimingsHistograms {
    pub execute_block_histogram: Histogram<u64>,
    pub execute_block_timings_histograms: ExecuteBlockTimingsHistograms,
}

impl ExecuteOperationsTimingsHistograms {
    pub fn new() -> Result<Self, ClientMetricsError> {
        Ok(Self {
            execute_block_histogram: Histogram::<u64>::new(2)?,
            execute_block_timings_histograms: ExecuteBlockTimingsHistograms::new()?,
        })
    }
}

pub struct BlockTimingsHistograms {
    pub execute_operations_histogram: Histogram<u64>,
    pub execute_operations_timings_histograms: ExecuteOperationsTimingsHistograms,
}

impl BlockTimingsHistograms {
    pub fn new() -> Result<Self, ClientMetricsError> {
        Ok(Self {
            execute_operations_histogram: Histogram::<u64>::new(2)?,
            execute_operations_timings_histograms: ExecuteOperationsTimingsHistograms::new()?,
        })
    }

    pub fn record_timing(
        &mut self,
        duration_ms: u64,
        timing_type: TimingType,
    ) -> Result<(), ClientMetricsError> {
        match timing_type {
            TimingType::ExecuteOperations => {
                self.execute_operations_histogram.record(duration_ms)?;
            }
            TimingType::ExecuteBlock => {
                self.execute_operations_timings_histograms
                    .execute_block_histogram
                    .record(duration_ms)?;
            }
            TimingType::SubmitBlockProposal => {
                self.execute_operations_timings_histograms
                    .execute_block_timings_histograms
                    .submit_block_proposal_histogram
                    .record(duration_ms)?;
            }
            TimingType::UpdateValidators => {
                self.execute_operations_timings_histograms
                    .execute_block_timings_histograms
                    .update_validators_histogram
                    .record(duration_ms)?;
            }
        }
        Ok(())
    }
}

#[cfg(not(web))]
pub struct ClientMetrics {
    pub timing_config: TimingConfig,
    pub timing_sender: mpsc::UnboundedSender<(u64, TimingType)>,
    pub timing_task: task::JoinHandle<()>,
}

#[cfg(not(web))]
impl ClientMetrics {
    pub fn new(timing_config: TimingConfig) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let timing_task = tokio::spawn(Self::timing_collection(
            rx,
            timing_config.report_interval_secs,
        ));

        Self {
            timing_config,
            timing_sender: tx,
            timing_task,
        }
    }

    async fn timing_collection(
        mut receiver: mpsc::UnboundedReceiver<(u64, TimingType)>,
        report_interval_secs: u64,
    ) {
        let mut histograms =
            BlockTimingsHistograms::new().expect("Failed to create timing histograms");

        let mut report_timer = time::interval(time::Duration::from_secs(report_interval_secs));
        report_timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                timing_data = receiver.recv() => {
                    match timing_data {
                        Some((duration_ms, timing_type)) => {
                            if let Err(e) = histograms.record_timing(duration_ms, timing_type) {
                                warn!("Failed to record timing data: {}", e);
                            }
                        }
                        None => {
                            debug!("Timing collection task shutting down - sender closed");
                            break;
                        }
                    }
                }
                _ = report_timer.tick() => {
                    Self::print_timing_report(&histograms);
                }
            }
        }
    }

    fn print_timing_report(histograms: &BlockTimingsHistograms) {
        for quantile in [0.99, 0.95, 0.90, 0.50] {
            let formatted_quantile = (quantile * 100.0) as usize;

            info!(
                "Execute operations p{}: {} ms",
                formatted_quantile,
                histograms
                    .execute_operations_histogram
                    .value_at_quantile(quantile)
            );

            info!(
                "  └─ Execute block p{}: {} ms",
                formatted_quantile,
                histograms
                    .execute_operations_timings_histograms
                    .execute_block_histogram
                    .value_at_quantile(quantile)
            );
            info!(
                "    ├─ Submit block proposal p{}: {} ms",
                formatted_quantile,
                histograms
                    .execute_operations_timings_histograms
                    .execute_block_timings_histograms
                    .submit_block_proposal_histogram
                    .value_at_quantile(quantile)
            );
            info!(
                "    └─ Update validators p{}: {} ms",
                formatted_quantile,
                histograms
                    .execute_operations_timings_histograms
                    .execute_block_timings_histograms
                    .update_validators_histogram
                    .value_at_quantile(quantile)
            );
        }
    }
}
