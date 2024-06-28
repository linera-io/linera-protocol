// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Resource consumption unit tests.

#![cfg(with_tokio_multi_thread)]

use futures::{channel::mpsc, StreamExt};
use linera_base::{
    data_types::{BlockHeight, Timestamp},
    identifiers::{ApplicationId, BytecodeId, ChainDescription, MessageId},
};
use linera_views::batch::Batch;

use super::{ApplicationStatus, SyncRuntimeHandle, SyncRuntimeInternal};
use crate::{
    execution_state_actor::Request, runtime::ResourceController, ContractRuntime,
    RawExecutionOutcome, UserContractInstance,
};

/// Test writing a batch of changes.
///
/// Ensure that resource consumption counts are updated correctly.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_write_batch() {
    let (runtime, mut execution_state_receiver) = create_contract_runtime();
    let mut runtime = SyncRuntimeHandle::new(runtime);
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

        let Request::WriteBatch {
            id,
            batch,
            callback,
        } = request
        else {
            panic!("Expected a `Request::WriteBatch` but got {request:?} instead");
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
    mpsc::UnboundedReceiver<Request>,
) {
    let chain_id = ChainDescription::Root(0).into();
    let (execution_state_sender, execution_state_receiver) = mpsc::unbounded();
    let resource_controller = ResourceController::default();

    let mut runtime = SyncRuntimeInternal::new(
        chain_id,
        BlockHeight(0),
        Timestamp::from(0),
        None,
        0,
        None,
        execution_state_sender,
        None,
        resource_controller,
        super::OracleResponses::Record(Vec::new()),
    );

    runtime.push_application(create_dummy_application());

    (runtime, execution_state_receiver)
}

/// Creates an [`ApplicationStatus`] for a dummy application.
fn create_dummy_application() -> ApplicationStatus {
    let chain_id = ChainDescription::Root(1).into();
    let id = ApplicationId {
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
    };

    ApplicationStatus {
        caller_id: None,
        id,
        parameters: vec![],
        signer: None,
        outcome: RawExecutionOutcome::default(),
    }
}
