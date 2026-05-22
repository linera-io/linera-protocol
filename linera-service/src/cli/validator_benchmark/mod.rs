// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Multi-layer pre-onboarding benchmark for a candidate validator.
//!
//! Tracking: linera-io/linera-infra#1198.

mod config;
mod latency;
mod report;

pub use config::Benchmark;

use anyhow::Result;
use linera_client::client_context::ClientContext;

impl Benchmark {
    pub async fn run(
        &self,
        _context: &mut ClientContext<
            impl linera_core::Environment<ValidatorNode = linera_rpc::Client>,
        >,
    ) -> Result<()> {
        anyhow::bail!("validator benchmark: not yet implemented")
    }
}
