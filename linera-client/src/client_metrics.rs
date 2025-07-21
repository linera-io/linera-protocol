// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use hdrhistogram::Histogram;
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

pub struct SubmitFastBlockProposalTimings {
    pub creating_proposal_ms: u64,
    pub stage_block_execution_ms: u64,
    pub creating_confirmed_block_ms: u64,
    pub submitting_block_proposal_ms: u64,
}

pub struct BlockTimeTimings {
    pub get_pending_message_bundles_ms: u64,
    pub submit_fast_block_proposal_ms: u64,
    pub submit_fast_block_proposal_timings: SubmitFastBlockProposalTimings,
    pub communicate_chain_updates_ms: u64,
}

pub struct BlockTimings {
    pub block_time_ms: u64,
    pub block_time_timings: BlockTimeTimings,
}

pub struct SubmitFastBlockProposalTimingsHistograms {
    pub creating_proposal_histogram: Histogram<u64>,
    pub stage_block_execution_histogram: Histogram<u64>,
    pub creating_confirmed_block_histogram: Histogram<u64>,
    pub submitting_block_proposal_histogram: Histogram<u64>,
}

impl SubmitFastBlockProposalTimingsHistograms {
    pub fn new() -> Result<Self, ClientMetricsError> {
        Ok(Self {
            creating_proposal_histogram: Histogram::<u64>::new(2)?,
            stage_block_execution_histogram: Histogram::<u64>::new(2)?,
            creating_confirmed_block_histogram: Histogram::<u64>::new(2)?,
            submitting_block_proposal_histogram: Histogram::<u64>::new(2)?,
        })
    }

    pub fn record(
        &mut self,
        submit_fast_block_proposal_timings: SubmitFastBlockProposalTimings,
    ) -> Result<(), ClientMetricsError> {
        self.creating_proposal_histogram
            .record(submit_fast_block_proposal_timings.creating_proposal_ms)?;
        self.stage_block_execution_histogram
            .record(submit_fast_block_proposal_timings.stage_block_execution_ms)?;
        self.creating_confirmed_block_histogram
            .record(submit_fast_block_proposal_timings.creating_confirmed_block_ms)?;
        self.submitting_block_proposal_histogram
            .record(submit_fast_block_proposal_timings.submitting_block_proposal_ms)?;
        Ok(())
    }
}

pub struct BlockTimeTimingsHistograms {
    pub get_pending_message_bundles_histogram: Histogram<u64>,
    pub submit_fast_block_proposal_histogram: Histogram<u64>,
    pub submit_fast_block_proposal_timings_histograms: SubmitFastBlockProposalTimingsHistograms,
    pub communicate_chain_updates_histogram: Histogram<u64>,
}

impl BlockTimeTimingsHistograms {
    pub fn new() -> Result<Self, ClientMetricsError> {
        Ok(Self {
            get_pending_message_bundles_histogram: Histogram::<u64>::new(2)?,
            submit_fast_block_proposal_histogram: Histogram::<u64>::new(2)?,
            submit_fast_block_proposal_timings_histograms:
                SubmitFastBlockProposalTimingsHistograms::new()?,
            communicate_chain_updates_histogram: Histogram::<u64>::new(2)?,
        })
    }

    pub fn record(
        &mut self,
        block_time_timings: BlockTimeTimings,
    ) -> Result<(), ClientMetricsError> {
        self.get_pending_message_bundles_histogram
            .record(block_time_timings.get_pending_message_bundles_ms)?;
        self.submit_fast_block_proposal_histogram
            .record(block_time_timings.submit_fast_block_proposal_ms)?;
        self.submit_fast_block_proposal_timings_histograms
            .record(block_time_timings.submit_fast_block_proposal_timings)?;
        self.communicate_chain_updates_histogram
            .record(block_time_timings.communicate_chain_updates_ms)?;
        Ok(())
    }
}

pub struct BlockTimingsHistograms {
    pub block_time_histogram: Histogram<u64>,
    pub block_time_timings_histograms: BlockTimeTimingsHistograms,
}

impl BlockTimingsHistograms {
    pub fn new() -> Result<Self, ClientMetricsError> {
        Ok(Self {
            block_time_histogram: Histogram::<u64>::new(2)?,
            block_time_timings_histograms: BlockTimeTimingsHistograms::new()?,
        })
    }

    pub fn record(&mut self, block_timings: BlockTimings) -> Result<(), ClientMetricsError> {
        self.block_time_histogram
            .record(block_timings.block_time_ms)?;
        self.block_time_timings_histograms
            .record(block_timings.block_time_timings)?;
        Ok(())
    }
}

#[cfg(not(web))]
pub struct ClientMetrics {
    pub timing_config: TimingConfig,
    pub timing_sender: mpsc::UnboundedSender<BlockTimings>,
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
        mut receiver: mpsc::UnboundedReceiver<BlockTimings>,
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
                        Some(block_timings) => {
                            if let Err(e) = histograms.record(block_timings) {
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
                "Block time p{}: {} ms",
                formatted_quantile,
                histograms.block_time_histogram.value_at_quantile(quantile)
            );

            info!(
                "  ├─ Get pending message bundles p{}: {} ms",
                formatted_quantile,
                histograms
                    .block_time_timings_histograms
                    .get_pending_message_bundles_histogram
                    .value_at_quantile(quantile)
            );
            info!(
                "  ├─ Submit fast block proposal p{}: {} ms",
                formatted_quantile,
                histograms
                    .block_time_timings_histograms
                    .submit_fast_block_proposal_histogram
                    .value_at_quantile(quantile)
            );
            info!(
                "  │  ├─ Creating proposal p{}: {} ms",
                formatted_quantile,
                histograms
                    .block_time_timings_histograms
                    .submit_fast_block_proposal_timings_histograms
                    .creating_proposal_histogram
                    .value_at_quantile(quantile)
            );
            info!(
                "  │  ├─ Stage block execution p{}: {} ms",
                formatted_quantile,
                histograms
                    .block_time_timings_histograms
                    .submit_fast_block_proposal_timings_histograms
                    .stage_block_execution_histogram
                    .value_at_quantile(quantile)
            );
            info!(
                "  │  ├─ Creating confirmed block p{}: {} ms",
                formatted_quantile,
                histograms
                    .block_time_timings_histograms
                    .submit_fast_block_proposal_timings_histograms
                    .creating_confirmed_block_histogram
                    .value_at_quantile(quantile)
            );
            info!(
                "  │  └─ Submitting block proposal p{}: {} ms",
                formatted_quantile,
                histograms
                    .block_time_timings_histograms
                    .submit_fast_block_proposal_timings_histograms
                    .submitting_block_proposal_histogram
                    .value_at_quantile(quantile)
            );
            info!(
                "  └─ Communicate chain updates p{}: {} ms",
                formatted_quantile,
                histograms
                    .block_time_timings_histograms
                    .communicate_chain_updates_histogram
                    .value_at_quantile(quantile)
            );
        }
    }
}
