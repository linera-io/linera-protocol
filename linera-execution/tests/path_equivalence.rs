// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Equivalence between the two block-execution paths.
//!
//! Each test runs the same operation twice — once through
//! [`ExecutionStateView::execute_operation`] (the threaded shared-memory path,
//! which spawns a long-lived block worker driven over an `mpsc` channel) and
//! once through [`ExecutionStateView::execute_operation_with_snapshots`] (the
//! snapshot-based per-action path, where each action spawns its own worker and
//! per-application snapshots flow in/out by value) — and asserts the produced
//! [`TransactionOutcome`] is identical between the two paths. This guards
//! against the two paths drifting apart: native uses the threaded one in
//! production, web uses the snapshot one, but on native both paths are
//! reachable and must agree.

use std::collections::BTreeMap;

use linera_base::identifiers::AccountOwner;
use linera_execution::{
    test_utils::{
        create_dummy_operation_context, ExpectedCall, RegisterMockApplication, SystemExecutionState,
    },
    Operation, ResourceController, TransactionTracker,
};

#[test_log::test(tokio::test)]
async fn threaded_and_snapshot_paths_match_for_simple_operation() -> anyhow::Result<()> {
    let dummy_op = vec![1, 2, 3];
    let dummy_result = vec![42, 13];

    // Build two parallel views with the same starting state and one mock app each.
    let (state_a, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view_threaded = state_a.into_view().await;
    let (app_id, app_threaded, blobs_threaded) = view_threaded.register_mock_application(0).await?;
    {
        let dummy_result = dummy_result.clone();
        app_threaded.expect_call(ExpectedCall::execute_operation(
            move |_runtime, _operation| Ok(dummy_result.clone()),
        ));
    }
    app_threaded.expect_call(ExpectedCall::default_finalize());

    let (state_b, _chain_id_b) = SystemExecutionState::dummy_chain_state(0);
    let mut view_snapshot = state_b.into_view().await;
    let (app_id_b, app_snapshot, blobs_snapshot) =
        view_snapshot.register_mock_application(0).await?;
    assert_eq!(
        app_id, app_id_b,
        "both views should produce the same deterministic application ID"
    );
    {
        let dummy_result = dummy_result.clone();
        app_snapshot.expect_call(ExpectedCall::execute_operation(
            move |_runtime, _operation| Ok(dummy_result.clone()),
        ));
    }
    app_snapshot.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let operation = Operation::User {
        application_id: app_id,
        bytes: dummy_op,
    };

    // Threaded shared-memory path.
    let mut tracker_threaded =
        TransactionTracker::new_replaying_blobs(blobs_threaded.iter().copied());
    let mut controller_threaded = ResourceController::<Option<AccountOwner>>::default();
    view_threaded
        .execute_operation(
            &mut tracker_threaded,
            &mut controller_threaded,
            context,
            operation.clone(),
        )
        .await?;
    let outcome_threaded = tracker_threaded.into_outcome()?;

    // Snapshot-based per-action path.
    let mut tracker_snapshot =
        TransactionTracker::new_replaying_blobs(blobs_snapshot.iter().copied());
    let mut controller_snapshot = ResourceController::<Option<AccountOwner>>::default();
    let mut block_snapshots: BTreeMap<_, Vec<u8>> = BTreeMap::new();
    view_snapshot
        .execute_operation_with_snapshots(
            &mut tracker_snapshot,
            &mut controller_snapshot,
            &mut block_snapshots,
            context,
            operation,
        )
        .await?;
    let outcome_snapshot = tracker_snapshot.into_outcome()?;

    assert_eq!(
        outcome_threaded, outcome_snapshot,
        "transaction outcomes must agree across paths"
    );
    assert_eq!(
        controller_threaded.tracker, controller_snapshot.tracker,
        "resource trackers must agree across paths"
    );

    Ok(())
}
