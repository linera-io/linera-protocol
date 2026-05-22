// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! L1 preflight: reachability, version, network description, baseline RTT.

use std::time::Instant;

use linera_core::node::ValidatorNode;

use super::{
    latency::Samples,
    report::{PreflightReport, PreflightStatus},
};

/// Number of lightweight round-trips used to estimate baseline RTT.
const PING_COUNT: usize = 10;

/// Outcome of a preflight run, including the raw version/description payloads so
/// the orchestrator can also populate report metadata.
pub struct PreflightOutcome {
    pub report: PreflightReport,
    /// Debug rendering of the validator's version info, if reachable.
    pub version_info: Option<String>,
    /// JSON rendering of the validator's network description, if reachable.
    pub network_description: Option<serde_json::Value>,
}

/// Run the preflight layer against a candidate node.
pub async fn run<N: ValidatorNode>(node: &N) -> PreflightOutcome {
    let mut report = PreflightReport::default();

    let version_info = match node.get_version_info().await {
        Ok(v) => Some(format!("{v:?}")),
        Err(e) => {
            report.errors.push(format!("get_version_info: {e}"));
            None
        }
    };

    let network_description = match node.get_network_description().await {
        Ok(nd) => serde_json::to_value(&nd).ok(),
        Err(e) => {
            report.errors.push(format!("get_network_description: {e}"));
            None
        }
    };

    // Baseline RTT from repeated lightweight calls.
    let mut rtt = Samples::new();
    for _ in 0..PING_COUNT {
        let start = Instant::now();
        match node.get_version_info().await {
            Ok(_) => rtt.record_success(start.elapsed().as_secs_f64() * 1000.0),
            Err(e) => rtt.record_error(categorize(&e.to_string())),
        }
    }
    report.rtt_ms = rtt.summary();

    report.status = if report.errors.is_empty() {
        PreflightStatus::Ok
    } else {
        PreflightStatus::Fail
    };

    PreflightOutcome {
        report,
        version_info,
        network_description,
    }
}

/// Bucket an error message into a coarse category for the report.
pub(super) fn categorize(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    if lower.contains("timeout") || lower.contains("timed out") {
        "timeout"
    } else if lower.contains("connection refused")
        || lower.contains("unavailable")
        || lower.contains("transport")
    {
        "unavailable"
    } else {
        "other"
    }
}

#[cfg(test)]
mod tests {
    use super::categorize;

    #[test]
    fn categorize_timeout() {
        assert_eq!(categorize("request timeout"), "timeout");
        assert_eq!(categorize("operation timed out"), "timeout");
    }

    #[test]
    fn categorize_unavailable() {
        assert_eq!(categorize("connection refused"), "unavailable");
        assert_eq!(categorize("Status: Unavailable"), "unavailable");
        assert_eq!(categorize("transport error"), "unavailable");
    }

    #[test]
    fn categorize_other() {
        assert_eq!(categorize("something weird"), "other");
    }
}
