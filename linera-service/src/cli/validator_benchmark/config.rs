// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! CLI definition for `linera validator benchmark`.

use linera_base::{crypto::ValidatorPublicKey, identifiers::ChainId};

/// Multi-layer pre-onboarding benchmark for a single candidate validator.
///
/// Probes the candidate across read-side primitives (preflight, baseline,
/// concurrency ramp, bulk download, tip lag) and emits a structured report.
/// The optional `--deep` layer additionally exercises the write path by
/// syncing a bounded number of blocks; it has a stateful side effect on the
/// candidate and is therefore off by default.
///
/// PREREQUISITE: the read layers are only meaningful if the candidate already
/// holds the `--chain` you pass. A not-yet-committee candidate may hold no
/// blocks; in that case pre-sync it (`linera validator sync`) or pass `--deep`,
/// which seeds the blocks first (and is run before the read layers). The tool
/// warns when a chain is not held.
#[derive(Debug, Clone, clap::Parser, serde::Serialize)]
pub struct Benchmark {
    /// Network address of the candidate validator (e.g. `grpcs://host:port`).
    pub address: String,

    /// Expected public key of the validator (identity verification).
    #[arg(long)]
    pub public_key: Option<ValidatorPublicKey>,

    /// Chain to exercise. Repeat for multiple chains. At least one required.
    #[arg(long, required = true)]
    pub chain: Vec<ChainId>,

    // --- Layer toggles ---
    /// Skip L1 preflight (version, network description, RTT).
    #[arg(long)]
    pub skip_preflight: bool,
    /// Skip L3 read latency baseline.
    #[arg(long)]
    pub skip_read_baseline: bool,
    /// Skip L4 read stress (concurrency ramp).
    #[arg(long)]
    pub skip_read_stress: bool,
    /// Skip L5 bulk certificate download.
    #[arg(long)]
    pub skip_bulk_download: bool,
    /// Skip L6 tip-lag snapshot.
    #[arg(long)]
    pub skip_tip_lag: bool,

    /// L2 partial sync (seed): sync a bounded run of blocks into the candidate,
    /// run before the read layers so they exercise real data. Stateful side
    /// effect on the candidate; off by default.
    #[arg(long)]
    pub deep: bool,

    // --- L3 (read latency baseline) ---
    /// Number of sequential chain-info queries per chain in L3.
    #[arg(long, default_value_t = 200)]
    pub baseline_requests: usize,

    // --- L4 (read stress / concurrency ramp) ---
    /// Concurrency levels for the L4 ramp.
    #[arg(long, value_delimiter = ',', default_value = "1,2,4,8,16,32,64")]
    pub stress_levels: Vec<usize>,
    /// Seconds to sustain each L4 concurrency level.
    #[arg(long, default_value_t = 30)]
    pub stress_duration_secs: u64,

    // --- L5 (bulk download) ---
    /// Number of heights per L5 download batch.
    #[arg(long, default_value_t = 100)]
    pub bulk_batch_size: u32,
    /// Concurrency levels for L5 bulk download.
    #[arg(long, value_delimiter = ',', default_value = "1,8")]
    pub bulk_concurrency: Vec<usize>,
    /// Either `auto` (last batch_size * 100 heights up to the candidate's tip)
    /// or an explicit `FROM:TO` range.
    #[arg(long, default_value = "auto")]
    pub bulk_height_range: String,

    // --- L6 (tip-lag snapshot) ---
    /// Number of tip-lag samples in L6.
    #[arg(long, default_value_t = 3)]
    pub tip_lag_samples: usize,
    /// Seconds between L6 tip-lag samples.
    #[arg(long, default_value_t = 120)]
    pub tip_lag_interval_secs: u64,

    // --- L2 (partial sync / seed, opt-in) ---
    /// Maximum number of blocks to seed in L2 partial sync (with `--deep`).
    #[arg(long, default_value_t = 1000)]
    pub deep_blocks: u32,
    /// Chain to use for the L2 partial-sync seed (defaults to the first `--chain`).
    #[arg(long)]
    pub deep_chain: Option<ChainId>,

    // --- Output ---
    /// Output spec, repeatable; or comma/+-separated within a single value.
    /// SPEC: `<format>` (stdout) or `<format>:<path>` (file).
    /// Formats: `json`, `yaml`, `md`, `brief`. Default if omitted: `md` to stdout.
    #[arg(long)]
    pub output: Vec<String>,

    /// Free-form tag carried in the report (e.g. `OVH US-EAST`).
    #[arg(long, default_value = "unspecified")]
    pub observer_location: String,

    /// Disable the interactive progress UI (auto-disabled when stderr is not a TTY).
    #[arg(long)]
    pub no_progress: bool,

    // --- Robustness ---
    /// Per-RPC timeout in seconds. A call that exceeds it is recorded as a
    /// `timeout` error and the run keeps going, so a hung validator never blocks.
    #[arg(long, default_value_t = 30)]
    pub rpc_timeout_secs: u64,
    /// Abort the run if preflight fails (default: continue and report).
    #[arg(long)]
    pub abort_on_preflight_fail: bool,
}
