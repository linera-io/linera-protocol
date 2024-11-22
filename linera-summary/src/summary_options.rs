// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use linera_version::VersionInfo;

#[derive(clap::Parser)]
#[command(
    name = "linera-summary",
    version = VersionInfo::default_clap_str(),
    about = "Executable for performance summary generation.",
)]
pub struct SummaryOptions {
    #[command(subcommand)]
    command: Command,

    /// The list of comma separated workflow names to track.
    #[arg(long, required = true)]
    workflows: String,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Run in CI mode.
    Ci,
    /// Run in local mode. Instead of commenting on the PR, the summary is printed to stdout.
    Local {
        /// PR number to analyze.
        #[arg(long, required = true)]
        pr: u64,
    },
}

impl SummaryOptions {
    pub fn init() -> Self {
        <SummaryOptions as clap::Parser>::parse()
    }

    pub fn is_local(&self) -> bool {
        matches!(self.command, Command::Local { .. })
    }

    pub fn pr_number(&self) -> Option<u64> {
        match self.command {
            Command::Local { pr } => Some(pr),
            Command::Ci { .. } => None,
        }
    }

    pub fn workflows(&self) -> HashSet<String> {
        self.workflows.split(',').map(|s| s.to_string()).collect()
    }
}
