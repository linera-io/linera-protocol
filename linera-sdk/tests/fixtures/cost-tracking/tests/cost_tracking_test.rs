// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Cost Tracking application.

#![cfg(not(target_arch = "wasm32"))]

use cost_tracking::{CostTrackingAbi, Operation, Query, QueryResponse};
use linera_sdk::test::TestValidator;

/// Test that runs all cost tracking operations and prints the fuel log.
#[tokio::test]
async fn test_cost_tracking() {
    let (validator, module_id) =
        TestValidator::with_current_module::<CostTrackingAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;

    // Create the application
    let application_id = chain
        .create_application(module_id, (), (), vec![])
        .await;

    // Execute the RunAll operation to perform all cost tracking operations
    let (certificate, resources) = chain
        .add_block(|block| {
            block.with_operation(application_id, Operation::RunAll);
        })
        .await;

    println!("=== Cost Tracking Test Results ===");

    // Query the logs
    let response = chain.query(application_id, Query::GetLogs).await.response;

    if let QueryResponse::Logs(logs) = response {
        println!("=== Fuel Consumption Log ===");
        println!("{:<40} {:>15}", "Operation", "Remaining Fuel");
        println!("{}", "-".repeat(56));

        let mut prev_fuel: Option<u64> = None;
        for entry in &logs {
            let diff = prev_fuel.map(|p| p.saturating_sub(entry.fuel));
            let diff_str = diff.map(|d| format!("(-{})", d)).unwrap_or_default();
            println!("{:<40} {:>15} {}", entry.label, entry.fuel, diff_str);
            prev_fuel = Some(entry.fuel);
        }

        println!();
        println!("Total log entries: {}", logs.len());

        // Calculate total fuel consumed
        if let (Some(first), Some(last)) = (logs.first(), logs.last()) {
            let total_consumed = first.fuel.saturating_sub(last.fuel);
            println!("Total fuel consumed: {}", total_consumed);
        }
    }

    // Also verify we can get the log count
    let count_response = chain.query(application_id, Query::GetLogCount).await.response;
    if let QueryResponse::LogCount(count) = count_response {
        println!("Log count from query: {}", count);
        assert!(count > 0, "Should have logged some entries");
    }
}
