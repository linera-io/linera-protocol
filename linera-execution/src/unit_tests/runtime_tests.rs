// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Resource consumption unit tests.

#![cfg(with_tokio_multi_thread)]

use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use futures::{channel::mpsc, StreamExt};
use linera_base::{
    data_types::{BlockHeight, Timestamp},
    identifiers::{ApplicationId, BytecodeId, ChainDescription, MessageId},
};
use linera_views::batch::Batch;

use super::{ApplicationStatus, SyncRuntimeHandle, SyncRuntimeInternal};
use crate::{
    execution_state_actor::ExecutionRequest,
    runtime::{LoadedApplication, ResourceController, SyncRuntime},
    ContractRuntime, RawExecutionOutcome, TransactionTracker, UserContractInstance,
};

/// Test if dropping [`SyncRuntime`] does not leak memory.
#[test_log::test(tokio::test)]
async fn test_dropping_sync_runtime_clears_loaded_applications() -> anyhow::Result<()> {
    let (runtime, _receiver) = create_runtime();
    let handle = SyncRuntimeHandle::from(runtime);
    let weak_handle = Arc::downgrade(&handle.0);

    let fake_application = create_fake_application_with_runtime(&handle);

    handle
        .0
        .try_lock()
        .expect("Failed to lock runtime")
        .loaded_applications
        .insert(create_dummy_application_id(), fake_application);

    let runtime = SyncRuntime(Some(handle));
    drop(runtime);
    assert!(weak_handle.upgrade().is_none());

    Ok(())
}

/// Test if [`SyncRuntime::into_inner`] fails if it would leak memory.
#[test_log::test(tokio::test)]
async fn test_into_inner_without_clearing_applications() {
    let (runtime, _receiver) = create_runtime();
    let handle = SyncRuntimeHandle::from(runtime);

    let fake_application = create_fake_application_with_runtime(&handle);

    handle
        .0
        .try_lock()
        .expect("Failed to lock runtime")
        .loaded_applications
        .insert(create_dummy_application_id(), fake_application);

    assert!(SyncRuntime(Some(handle)).into_inner().is_none());
}

/// Test if [`SyncRuntime::into_inner`] succeeds if loaded applications have been cleared.
#[test_log::test(tokio::test)]
async fn test_into_inner_after_clearing_applications() {
    let (runtime, _receiver) = create_runtime();
    let handle = SyncRuntimeHandle::from(runtime);
    let weak_handle = Arc::downgrade(&handle.0);

    let fake_application = create_fake_application_with_runtime(&handle);

    {
        let mut runtime = handle.0.try_lock().expect("Failed to lock runtime");
        runtime
            .loaded_applications
            .insert(create_dummy_application_id(), fake_application);
        runtime.loaded_applications.clear();
    }

    assert!(SyncRuntime(Some(handle)).into_inner().is_some());
    assert!(weak_handle.upgrade().is_none());
}

/// Test writing a batch of changes.
///
/// Ensure that resource consumption counts are updated correctly.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_write_batch() {
    let (runtime, mut execution_state_receiver) = create_contract_runtime();
    let mut runtime = SyncRuntimeHandle::from(runtime);
    let mut batch = Batch::new();

    let write_key = vec![1, 2, 3, 4, 5];
    let write_data = vec![6, 7, 8, 9];
    let delete_key = vec![10, 11, 12];
    let delete_key_prefix = vec![13, 14, 15, 16, 17, 18];

    let expected_bytes_count =
        write_key.len() + write_data.len() + delete_key.len() + delete_key_prefix.len();

    batch.put_key_value_bytes(write_key, write_data);
    batch.delete_key(delete_key);
    batch.delete_key_prefix(delete_key_prefix);

    let expected_write_count = batch.operations.len();
    let expected_application_id = runtime.inner().current_application().id;
    let expected_batch = batch.clone();

    tokio::spawn(async move {
        let request = execution_state_receiver
            .next()
            .await
            .expect("Missing expected request to write a batch");

        let ExecutionRequest::WriteBatch {
            id,
            batch,
            callback,
        } = request
        else {
            panic!("Expected a `ExecutionRequest::WriteBatch` but got {request:?} instead");
        };

        assert_eq!(id, expected_application_id);
        assert_eq!(batch, expected_batch);

        callback
            .send(())
            .expect("Failed to notify that writing the batch finished");
    });

    runtime
        .write_batch(batch)
        .expect("Failed to write test batch");

    assert_eq!(
        runtime.inner().resource_controller.tracker.write_operations,
        expected_write_count as u32
    );
    assert_eq!(
        runtime.inner().resource_controller.tracker.bytes_written,
        expected_bytes_count as u64
    );
}

/// Creates a [`SyncRuntimeInternal`] instance for contracts, and returns it and the receiver
/// endpoint for the requests the runtime sends to the [`ExecutionStateView`] actor.
fn create_contract_runtime() -> (
    SyncRuntimeInternal<UserContractInstance>,
    mpsc::UnboundedReceiver<ExecutionRequest>,
) {
    let (mut runtime, execution_state_receiver) = create_runtime();

    runtime.push_application(create_dummy_application());

    (runtime, execution_state_receiver)
}

/// Creates a [`SyncRuntimeInternal`] instance for custom `Application` types (which can
/// be invalid types).
///
/// Returns the [`SyncRuntimeInternal`] instance and the receiver endpoint for the requests the
/// runtime sends to the [`ExecutionStateView`] actor.
fn create_runtime<Application>() -> (
    SyncRuntimeInternal<Application>,
    mpsc::UnboundedReceiver<ExecutionRequest>,
) {
    let chain_id = ChainDescription::Root(0).into();
    let (execution_state_sender, execution_state_receiver) = mpsc::unbounded();
    let resource_controller = ResourceController::default();

    let runtime = SyncRuntimeInternal::new(
        chain_id,
        BlockHeight(0),
        Timestamp::from(0),
        None,
        None,
        execution_state_sender,
        None,
        resource_controller,
        TransactionTracker::new(0, Some(Vec::new())),
    );

    (runtime, execution_state_receiver)
}

/// Creates an [`ApplicationStatus`] for a dummy application.
fn create_dummy_application() -> ApplicationStatus {
    ApplicationStatus {
        caller_id: None,
        id: create_dummy_application_id(),
        parameters: vec![],
        signer: None,
        outcome: RawExecutionOutcome::default(),
    }
}

/// Creates a dummy [`ApplicationId`].
fn create_dummy_application_id() -> ApplicationId {
    let chain_id = ChainDescription::Root(1).into();

    ApplicationId {
        bytecode_id: BytecodeId::new(MessageId {
            chain_id,
            height: BlockHeight(1),
            index: 0,
        }),
        creation: MessageId {
            chain_id,
            height: BlockHeight(1),
            index: 1,
        },
    }
}

/// Creates a fake application instance that's just a reference to the `runtime`.
fn create_fake_application_with_runtime(
    runtime: &SyncRuntimeHandle<Arc<dyn Any + Send + Sync>>,
) -> LoadedApplication<Arc<dyn Any + Send + Sync>> {
    let fake_instance: Arc<dyn Any + Send + Sync> = runtime.0.clone();

    LoadedApplication {
        instance: Arc::new(Mutex::new(fake_instance)),
        parameters: vec![],
    }
}
