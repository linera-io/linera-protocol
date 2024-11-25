// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::process;

use anyhow::{Context, Result};
use linera_summary::{
    github::Github, performance_summary::PerformanceSummary, summary_options::SummaryOptions,
};
use tracing::{error, Instrument};

async fn run(options: SummaryOptions) -> Result<()> {
    let tracked_workflows = options.workflows();
    let github = Github::new(options.is_local(), options.pr_number())?;
    let summary = PerformanceSummary::init(github, tracked_workflows).await?;
    summary.upsert_pr_comment().await?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let options = SummaryOptions::init();

    linera_base::tracing::init("summary");

    let mut runtime = tokio::runtime::Builder::new_multi_thread();

    let span = tracing::info_span!("linera-summary::main");

    let result = runtime
        .enable_all()
        .build()
        .context("Failed to create Tokio runtime")?
        .block_on(run(options).instrument(span));

    let error_code = match result {
        Ok(()) => 0,
        Err(msg) => {
            error!("Error: {msg:?}");
            2
        }
    };
    process::exit(error_code);
}
