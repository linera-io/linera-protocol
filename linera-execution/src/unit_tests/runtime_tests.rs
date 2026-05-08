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
    crypto::CryptoHash,
    data_types::{Amount, BlockHeight},
    identifiers::{Account, AccountOwner, ApplicationId},
};
use linera_views::batch::Batch;

use super::{ApplicationStatus, SyncRuntimeHandle, SyncRuntimeInternal, WithContext};
use crate::{
    execution_state_actor::ExecutionRequest,
    runtime::{ContractSyncRuntimeHandle, LoadedApplication, ResourceController, SyncRuntime},
    test_utils::{create_dummy_user_application_description, dummy_chain_description},
    ContractRuntime, ExecutionError, UserContractInstance,
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
fn create_runtime<Application: WithContext>() -> (
    SyncRuntimeInternal<Application>,
    mpsc::UnboundedReceiver<ExecutionRequest>,
)
where
    Application::UserContext: Default,
{
    let chain_id = dummy_chain_description(0).id();
    let (execution_state_sender, execution_state_receiver) = mpsc::unbounded();
    let resource_controller = ResourceController::default();

    let runtime = SyncRuntimeInternal::new(
        chain_id,
        BlockHeight(0),
        Some(0),
        None,
        execution_state_sender,
        None,
        None,
        resource_controller,
        Default::default(),
        true,
    );

    (runtime, execution_state_receiver)
}

/// Creates an [`ApplicationStatus`] for a dummy application.
fn create_dummy_application() -> ApplicationStatus {
    create_dummy_application_with_index(0)
}

/// Creates an [`ApplicationStatus`] for a dummy application identified by `index`. Each
/// distinct `index` yields a distinct [`ApplicationId`].
fn create_dummy_application_with_index(index: u32) -> ApplicationStatus {
    let (description, _, _) = create_dummy_user_application_description(index);
    let id = From::from(&description);
    ApplicationStatus {
        caller_id: None,
        id,
        description,
        signer: None,
    }
}

/// Creates a dummy [`ApplicationId`].
fn create_dummy_application_id() -> ApplicationId {
    ApplicationId::new(CryptoHash::test_hash("application description"))
}

/// Creates a [`ContractSyncRuntimeHandle`] with a single dummy application on the call stack.
fn create_handle_with_single_application() -> (
    ContractSyncRuntimeHandle,
    mpsc::UnboundedReceiver<ExecutionRequest>,
    ApplicationId,
) {
    let (runtime, receiver) = create_contract_runtime();
    let application_id = runtime.current_application().id;
    (SyncRuntimeHandle::from(runtime), receiver, application_id)
}

/// Creates a [`ContractSyncRuntimeHandle`] with two distinct applications on the call stack:
/// `caller` is at depth 1, `callee` is the current (depth 0) application.
fn create_handle_with_caller_and_callee() -> (
    ContractSyncRuntimeHandle,
    mpsc::UnboundedReceiver<ExecutionRequest>,
    ApplicationId, // caller (depth 1)
    ApplicationId, // callee (depth 0)
) {
    let (mut runtime, receiver) = create_runtime();
    let caller = create_dummy_application_with_index(0);
    let callee = create_dummy_application_with_index(1);
    let caller_id = caller.id;
    let callee_id = callee.id;
    runtime.push_application(caller);
    runtime.push_application(callee);
    (
        SyncRuntimeHandle::from(runtime),
        receiver,
        caller_id,
        callee_id,
    )
}

/// Helper that returns a dummy `(source_owner, destination_account, amount)` triple.
fn dummy_transfer_args() -> (AccountOwner, Account, Amount) {
    let source = AccountOwner::CHAIN;
    let destination = Account::new(dummy_chain_description(0).id(), AccountOwner::CHAIN);
    let amount = Amount::from_tokens(1);
    (source, destination, amount)
}

/// `transfer_auth_depth(.., 0)` is equivalent to `transfer(..)`: both stamp the request with
/// the current application's id.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn transfer_auth_depth_zero_uses_current_application() {
    let (mut handle, mut receiver, current_id) = create_handle_with_single_application();
    let (source, destination, amount) = dummy_transfer_args();

    tokio::spawn(async move {
        let request = receiver.next().await.expect("missing Transfer request");
        let ExecutionRequest::Transfer {
            application_id,
            callback,
            ..
        } = request
        else {
            panic!("Expected ExecutionRequest::Transfer, got {request:?}");
        };
        assert_eq!(application_id, current_id);
        callback.send(()).expect("Failed to ack Transfer");
    });

    handle
        .transfer_auth_depth(source, destination, amount, 0)
        .expect("transfer_auth_depth(0) should succeed");
}

/// `transfer_auth_depth(.., 1)` stamps the request with the caller's id, not the current
/// application's id.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn transfer_auth_depth_one_uses_caller() {
    let (mut handle, mut receiver, caller_id, callee_id) = create_handle_with_caller_and_callee();
    assert_ne!(caller_id, callee_id);
    let (source, destination, amount) = dummy_transfer_args();

    tokio::spawn(async move {
        let request = receiver.next().await.expect("missing Transfer request");
        let ExecutionRequest::Transfer {
            application_id,
            callback,
            ..
        } = request
        else {
            panic!("Expected ExecutionRequest::Transfer, got {request:?}");
        };
        assert_eq!(application_id, caller_id);
        callback.send(()).expect("Failed to ack Transfer");
    });

    handle
        .transfer_auth_depth(source, destination, amount, 1)
        .expect("transfer_auth_depth(1) should succeed");
}

/// `transfer_auth_depth(.., depth)` returns `AuthDepthOutOfRange` when `depth` exceeds the
/// current call stack.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn transfer_auth_depth_out_of_range() {
    let (mut handle, _receiver, _) = create_handle_with_single_application();
    let (source, destination, amount) = dummy_transfer_args();

    let error = handle
        .transfer_auth_depth(source, destination, amount, 1)
        .expect_err("depth 1 with a single-frame stack must fail");
    match error {
        ExecutionError::AuthDepthOutOfRange { depth } => assert_eq!(depth, 1),
        other => panic!("Expected AuthDepthOutOfRange, got {other:?}"),
    }
}

/// `claim_auth_depth(.., 1)` stamps the request with the caller's id.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn claim_auth_depth_one_uses_caller() {
    let (mut handle, mut receiver, caller_id, _callee_id) = create_handle_with_caller_and_callee();
    let chain = dummy_chain_description(0).id();
    let source = Account::new(chain, AccountOwner::CHAIN);
    let destination = Account::new(chain, AccountOwner::CHAIN);
    let amount = Amount::from_tokens(1);

    tokio::spawn(async move {
        let request = receiver.next().await.expect("missing Claim request");
        let ExecutionRequest::Claim {
            application_id,
            callback,
            ..
        } = request
        else {
            panic!("Expected ExecutionRequest::Claim, got {request:?}");
        };
        assert_eq!(application_id, caller_id);
        callback.send(()).expect("Failed to ack Claim");
    });

    handle
        .claim_auth_depth(source, destination, amount, 1)
        .expect("claim_auth_depth(1) should succeed");
}

/// `claim_auth_depth(.., depth)` errors out when `depth` exceeds the call stack.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn claim_auth_depth_out_of_range() {
    let (mut handle, _receiver, _) = create_handle_with_single_application();
    let chain = dummy_chain_description(0).id();
    let source = Account::new(chain, AccountOwner::CHAIN);
    let destination = Account::new(chain, AccountOwner::CHAIN);
    let amount = Amount::from_tokens(1);

    let error = handle
        .claim_auth_depth(source, destination, amount, 5)
        .expect_err("depth 5 with a single-frame stack must fail");
    match error {
        ExecutionError::AuthDepthOutOfRange { depth } => assert_eq!(depth, 5),
        other => panic!("Expected AuthDepthOutOfRange, got {other:?}"),
    }
}

/// `transfer_from_auth_depth(.., 1)` stamps the request with the caller's id.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn transfer_from_auth_depth_one_uses_caller() {
    let (mut handle, mut receiver, caller_id, _callee_id) = create_handle_with_caller_and_callee();
    let owner = AccountOwner::CHAIN;
    let spender = AccountOwner::CHAIN;
    let destination = Account::new(dummy_chain_description(0).id(), AccountOwner::CHAIN);
    let amount = Amount::from_tokens(1);

    tokio::spawn(async move {
        let request = receiver.next().await.expect("missing TransferFrom request");
        let ExecutionRequest::TransferFrom {
            application_id,
            callback,
            ..
        } = request
        else {
            panic!("Expected ExecutionRequest::TransferFrom, got {request:?}");
        };
        assert_eq!(application_id, caller_id);
        callback.send(()).expect("Failed to ack TransferFrom");
    });

    handle
        .transfer_from_auth_depth(owner, spender, destination, amount, 1)
        .expect("transfer_from_auth_depth(1) should succeed");
}

/// `transfer_from_auth_depth(.., depth)` errors out when `depth` exceeds the call stack.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn transfer_from_auth_depth_out_of_range() {
    let (mut handle, _receiver, _) = create_handle_with_single_application();
    let destination = Account::new(dummy_chain_description(0).id(), AccountOwner::CHAIN);

    let error = handle
        .transfer_from_auth_depth(
            AccountOwner::CHAIN,
            AccountOwner::CHAIN,
            destination,
            Amount::from_tokens(1),
            2,
        )
        .expect_err("depth 2 with a single-frame stack must fail");
    match error {
        ExecutionError::AuthDepthOutOfRange { depth } => assert_eq!(depth, 2),
        other => panic!("Expected AuthDepthOutOfRange, got {other:?}"),
    }
}

/// Creates a fake application instance that's just a reference to the `runtime`.
fn create_fake_application_with_runtime(
    runtime: &SyncRuntimeHandle<Arc<dyn Any + Send + Sync>>,
) -> LoadedApplication<Arc<dyn Any + Send + Sync>> {
    let fake_instance: Arc<dyn Any + Send + Sync> = runtime.0.clone();
    let (description, _, _) = create_dummy_user_application_description(0);

    LoadedApplication {
        instance: Arc::new(Mutex::new(fake_instance)),
        description,
    }
}
