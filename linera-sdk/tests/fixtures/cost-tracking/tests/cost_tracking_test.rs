// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Cost Tracking application.

#![cfg(not(target_arch = "wasm32"))]

use cost_tracking::{CostTrackingAbi, LogEntry, Operation, Query, QueryResponse};
use linera_sdk::test::TestValidator;

/// Computes fuel consumption differences between consecutive log entries.
/// Returns a vector of (label, fuel_consumed) pairs.
fn compute_fuel_diffs(logs: &[LogEntry]) -> Vec<(String, u64)> {
    logs.windows(2)
        .map(|window| {
            let prev = &window[0];
            let curr = &window[1];
            let fuel_consumed = prev.fuel.saturating_sub(curr.fuel);
            (curr.label.clone(), fuel_consumed)
        })
        .collect()
}

/// Formats fuel differences as a snapshot-friendly string.
fn format_fuel_diffs(diffs: &[(String, u64)]) -> String {
    let mut lines: Vec<String> = Vec::new();
    for (label, fuel_consumed) in diffs {
        lines.push(format!("{}: {}", label, fuel_consumed));
    }
    lines.join("\n")
}

/// Test that runs all cost tracking operations and compares fuel consumption against snapshots.
#[tokio::test]
async fn test_cost_tracking() {
    let (validator, module_id) =
        TestValidator::with_current_module::<CostTrackingAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;

    // Create the application
    let application_id = chain.create_application(module_id, (), (), vec![]).await;

    // Execute the RunAll operation to perform all cost tracking operations
    chain
        .add_block(|block| {
            block.with_operation(application_id, Operation::RunAll);
        })
        .await;

    // Query the logs
    let response = chain.query(application_id, Query::GetLogs).await.response;

    let QueryResponse::Logs(logs) = response else {
        panic!("Expected Logs response");
    };

    // Compute fuel differences between consecutive measurements
    let diffs = compute_fuel_diffs(&logs);
    let formatted = format_fuel_diffs(&diffs);

    // Snapshot the fuel consumption differences
    // When fuel usage changes, this test will fail and show exactly what changed
    insta::assert_snapshot!("fuel_consumption_diffs", formatted);

    // Verify we have log entries
    assert!(!logs.is_empty(), "Should have logged some entries");

    // Also verify we can get the log count
    let count_response = chain
        .query(application_id, Query::GetLogCount)
        .await
        .response;
    if let QueryResponse::LogCount(count) = count_response {
        assert!(count > 0, "Should have logged some entries");
    }
}
