// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use hdrhistogram::Histogram;
use linera_core::client::TimingType;
use tokio::{sync::mpsc, task, time};
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
/// Configuration for client-side timing metrics collection and reporting.
pub struct TimingConfig {
    /// Whether timing metrics collection is enabled.
    pub enabled: bool,
    /// How often, in seconds, the collected timing data is reported.
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
/// Errors that can occur while creating or recording client timing metrics.
pub enum ClientMetricsError {
    /// A histogram could not be created.
    #[error("Failed to create histogram: {0}")]
    HistogramCreationError(#[from] hdrhistogram::CreationError),
    /// A value could not be recorded into a histogram.
    #[error("Failed to record histogram: {0}")]
    HistogramRecordError(#[from] hdrhistogram::RecordError),
}

/// Histograms tracking the time spent in the individual phases of executing a block.
pub struct ExecuteBlockTimingsHistograms {
    /// Time taken to submit a block proposal, in milliseconds.
    pub submit_block_proposal_histogram: Histogram<u64>,
    /// Time taken to update the validators, in milliseconds.
    pub update_validators_histogram: Histogram<u64>,
}

impl ExecuteBlockTimingsHistograms {
    /// Creates a new set of block-phase timing histograms.
    pub fn new() -> Result<Self, ClientMetricsError> {
        Ok(Self {
            submit_block_proposal_histogram: Histogram::<u64>::new(2)?,
            update_validators_histogram: Histogram::<u64>::new(2)?,
        })
    }
}

/// Histograms tracking the time spent executing operations, broken down by sub-phase.
pub struct ExecuteOperationsTimingsHistograms {
    /// Time taken to execute a single block, in milliseconds.
    pub execute_block_histogram: Histogram<u64>,
    /// Nested histograms for the block-execution sub-phases.
    pub execute_block_timings_histograms: ExecuteBlockTimingsHistograms,
}

impl ExecuteOperationsTimingsHistograms {
    /// Creates a new set of operation-execution timing histograms.
    pub fn new() -> Result<Self, ClientMetricsError> {
        Ok(Self {
            execute_block_histogram: Histogram::<u64>::new(2)?,
            execute_block_timings_histograms: ExecuteBlockTimingsHistograms::new()?,
        })
    }
}

/// Histograms tracking the time spent processing a whole block, broken down by phase.
pub struct BlockTimingsHistograms {
    /// Time taken to execute the operations in a block, in milliseconds.
    pub execute_operations_histogram: Histogram<u64>,
    /// Nested histograms for the operation-execution sub-phases.
    pub execute_operations_timings_histograms: ExecuteOperationsTimingsHistograms,
}

impl BlockTimingsHistograms {
    /// Creates a new set of block timing histograms.
    pub fn new() -> Result<Self, ClientMetricsError> {
        Ok(Self {
            execute_operations_histogram: Histogram::<u64>::new(2)?,
            execute_operations_timings_histograms: ExecuteOperationsTimingsHistograms::new()?,
        })
    }

    /// Records a measured duration into the histogram matching the given timing type.
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

/// Collects client-side timing metrics and reports them periodically.
#[cfg(not(web))]
pub struct ClientMetrics {
    /// Configuration controlling whether timing collection is enabled and how often it reports.
    pub timing_config: TimingConfig,
    /// Sender used to forward measured durations and their timing type to the collection task.
    pub timing_sender: mpsc::UnboundedSender<(u64, TimingType)>,
    /// Handle to the background task that aggregates and reports timing data.
    pub timing_task: task::JoinHandle<()>,
}

#[cfg(not(web))]
impl ClientMetrics {
    /// Creates a new client metrics collector and spawns its background reporting task.
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

        let mut report_needed = false;
        let mut report_timer = time::interval(time::Duration::from_secs(report_interval_secs));
        report_timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                timing_data = receiver.recv() => {
                    match timing_data {
                        Some((duration_ms, timing_type)) => {
                            if let Err(e) = histograms.record_timing(duration_ms, timing_type) {
                                warn!("Failed to record timing data: {}", e);
                            } else {
                                report_needed = true;
                            }
                        }
                        None => {
                            debug!("Timing collection task shutting down - sender closed");
                            break;
                        }
                    }
                }
                _ = report_timer.tick() => {
                    if report_needed {
                        Self::print_timing_report(&histograms);
                        report_needed = false;
                    }
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
