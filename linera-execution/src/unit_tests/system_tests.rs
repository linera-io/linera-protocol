// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use linera_base::data_types::{Blob, BlockHeight, Bytecode};
#[cfg(with_testing)]
use linera_base::vm::VmRuntime;
use linera_views::context::MemoryContext;

use super::*;
use crate::{
    test_utils::dummy_chain_description, ExecutionStateView, Message, MessageContext,
    PreparedCheckpoint, TestExecutionRuntimeContext,
};

/// Returns an execution state view and a matching operation context, for epoch 1, with root
/// chain 0 as the admin ID and one empty committee.
async fn new_view_and_context() -> (
    ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>,
    OperationContext,
) {
    let description = dummy_chain_description(5);
    let context = OperationContext {
        chain_id: ChainId::from(&description),
        authenticated_owner: None,
        height: BlockHeight::from(7),
        round: Some(0),
        timestamp: Default::default(),
    };
    let state = SystemExecutionState {
        description: Some(description),
        epoch: Epoch(1),
        admin_chain_id: Some(dummy_chain_description(0).id()),
        committees: BTreeMap::new(),
        ..SystemExecutionState::default()
    };
    let view = state.into_view().await;
    (view, context)
}

fn expected_application_id(
    context: &OperationContext,
    module_id: &ModuleId,
    parameters: Vec<u8>,
    required_application_ids: Vec<ApplicationId>,
    application_index: u32,
) -> ApplicationId {
    let description = ApplicationDescription {
        module_id: *module_id,
        creator_chain_id: context.chain_id,
        block_height: context.height,
        application_index,
        parameters,
        required_application_ids,
    };
    From::from(&description)
}

#[tokio::test]
async fn application_message_index() -> anyhow::Result<()> {
    let (mut view, context) = new_view_and_context().await;
    let contract = Bytecode::new(b"contract".into());
    let service = Bytecode::new(b"service".into());
    let contract_blob = Blob::new_contract_bytecode(contract.compress());
    let service_blob = Blob::new_service_bytecode(service.compress());
    let vm_runtime = VmRuntime::Wasm;
    let module_id = ModuleId::new(contract_blob.id().hash, service_blob.id().hash, vm_runtime);

    let operation = SystemOperation::CreateApplication {
        module_id,
        parameters: vec![],
        instantiation_argument: vec![],
        required_application_ids: vec![],
    };
    let mut txn_tracker = TransactionTracker::default();
    view.context()
        .extra()
        .add_blobs([contract_blob, service_blob])
        .await?;
    let mut controller = ResourceController::default();
    let new_application = view
        .system
        .execute_operation(context, operation, &mut txn_tracker, &mut controller)
        .await?;
    let id = expected_application_id(&context, &module_id, vec![], vec![], 0);
    assert_eq!(new_application, Some((id, vec![])));

    Ok(())
}

#[tokio::test]
async fn open_chain_message_index() {
    let (mut view, context) = new_view_and_context().await;
    let owner = linera_base::crypto::AccountPublicKey::test_key(0).into();
    let ownership = ChainOwnership::single(owner);
    let config = OpenChainConfig {
        ownership,
        balance: Amount::ZERO,
        application_permissions: Default::default(),
    };
    let mut txn_tracker = TransactionTracker::default();
    let operation = SystemOperation::OpenChain(config.clone());
    let mut controller = ResourceController::default();
    let new_application = view
        .system
        .execute_operation(context, operation, &mut txn_tracker, &mut controller)
        .await
        .unwrap();
    assert_eq!(new_application, None);
    assert_eq!(
        txn_tracker.into_outcome().unwrap().blobs[0].id().blob_type,
        BlobType::ChainDescription,
    );
}

/// Tests if an account is removed from storage if it is drained.
#[tokio::test]
async fn empty_accounts_are_removed() -> anyhow::Result<()> {
    let owner = AccountOwner::from(CryptoHash::test_hash("account owner"));
    let amount = Amount::from_tokens(99);

    let mut view = SystemExecutionState {
        description: Some(dummy_chain_description(0)),
        balances: BTreeMap::from([(owner, amount)]),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    view.system.debit(&owner, amount).await?;

    assert!(view.system.balances.indices().await?.is_empty());

    Ok(())
}

#[tokio::test]
async fn execute_checkpoint_publishes_blob_and_records_oracle_response() -> anyhow::Result<()> {
    use linera_base::data_types::OracleResponse;
    use linera_views::{batch::Batch, store::WritableKeyValueStore as _, views::View as _};

    let mut view = SystemExecutionState {
        description: Some(dummy_chain_description(0)),
        balance: Amount::from_tokens(100),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    // Persist the initial state so dump_content reads non-default bytes from storage.
    let mut batch = Batch::new();
    view.pre_save(&mut batch)?;
    view.context().store().write_batch(batch).await?;
    view.post_save();

    let pre_checkpoint_hash = view.crypto_hash_mut().await?;

    let mut txn_tracker = TransactionTracker::default();
    let blobs = view.prepare_checkpoint(u64::MAX).await?;
    view.apply_checkpoint(
        PreparedCheckpoint {
            blobs,
            origin_cursors: Vec::new(),
            outbox_block_hashes: Vec::new(),
        },
        &mut txn_tracker,
    )
    .await?;

    // The override hash takes effect immediately for the in-memory view.
    let post_checkpoint_hash = view.crypto_hash_mut().await?;
    assert_ne!(pre_checkpoint_hash, post_checkpoint_hash);

    let outcome = txn_tracker.into_outcome()?;
    assert_eq!(outcome.blobs.len(), 1);
    let blob = &outcome.blobs[0];
    assert_eq!(blob.id().blob_type, BlobType::CheckpointExecutionState);
    assert!(outcome.blobs_published.is_empty());
    assert_eq!(outcome.oracle_responses.len(), 1);
    assert_eq!(
        outcome.oracle_responses[0],
        OracleResponse::Checkpoint {
            execution_state_blobs: vec![blob.id().hash],
            used_blobs: vec![],
            outbox_block_hashes: vec![],
        }
    );

    // Saving the chain commits the override; the persisted hash now matches the content hash.
    let mut batch = Batch::new();
    view.pre_save(&mut batch)?;
    view.context().store().write_batch(batch).await?;
    view.post_save();
    assert_eq!(view.crypto_hash_mut().await?, post_checkpoint_hash);

    Ok(())
}

#[tokio::test]
async fn checkpoint_roundtrip_via_separate_view_yields_matching_hash() -> anyhow::Result<()> {
    use linera_views::{
        batch::Batch, context::MemoryContext, store::WritableKeyValueStore as _, views::View as _,
    };

    use crate::ExecutionStateView;

    // Producer side: build a view with non-trivial state, save, prepare-and-apply a
    // checkpoint, save with the override hash.
    let mut producer = SystemExecutionState {
        description: Some(dummy_chain_description(0)),
        balance: Amount::from_tokens(100),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;
    let mut batch = Batch::new();
    producer.pre_save(&mut batch)?;
    producer.context().store().write_batch(batch).await?;
    producer.post_save();

    // Use a small chunk size so the dump spans multiple `CheckpointExecutionState`
    // blobs. The bootstrap side concatenates them in restore order.
    const CHUNK_SIZE: usize = 32;
    let blobs = producer.prepare_checkpoint(CHUNK_SIZE as u64).await?;
    assert!(
        blobs.len() > 1,
        "Test state should be large enough to span multiple chunks",
    );
    for blob in &blobs {
        assert!(blob.bytes().len() <= CHUNK_SIZE);
    }
    let blob_bytes = blobs
        .iter()
        .flat_map(|blob| blob.bytes().iter().copied())
        .collect::<Vec<u8>>();
    let mut txn_tracker = TransactionTracker::default();
    producer
        .apply_checkpoint(
            PreparedCheckpoint {
                blobs,
                origin_cursors: Vec::new(),
                outbox_block_hashes: Vec::new(),
            },
            &mut txn_tracker,
        )
        .await?;
    let mut batch = Batch::new();
    producer.pre_save(&mut batch)?;
    producer.context().store().write_batch(batch).await?;
    producer.post_save();
    let producer_hash = producer.crypto_hash_mut().await?;
    drop(producer);

    // Bootstrap side: a fresh, separate view (different storage backend, same chain
    // description so the test contexts agree on `chain_id`). Restore the checkpoint
    // blob's bytes, then reload the view. Its execution-state hash must match the
    // producer's — that's the contract a bootstrapping node relies on.
    let mut bootstrap = SystemExecutionState {
        description: Some(dummy_chain_description(0)),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;
    let bootstrap_context: MemoryContext<TestExecutionRuntimeContext> = bootstrap.context().clone();
    bootstrap.restore_from_content(&blob_bytes).await?;
    drop(bootstrap);
    let mut reloaded = ExecutionStateView::load(bootstrap_context).await?;
    assert_eq!(reloaded.crypto_hash_mut().await?, producer_hash);

    Ok(())
}

#[tokio::test]
async fn checkpoint_notifies_origins_and_receive_records_finalization() -> anyhow::Result<()> {
    use linera_base::{crypto::CryptoHash, data_types::Cursor};

    let origin_a = dummy_chain_description(1).id();
    let origin_b = dummy_chain_description(2).id();
    let cursor_a = Cursor {
        height: BlockHeight::from(7),
        index: 3,
    };
    let cursor_b = Cursor {
        height: BlockHeight::from(11),
        index: 0,
    };

    // Producer side: apply a checkpoint with non-empty origin_cursors and check that
    // it queues one `SystemMessage::CheckpointAck` per origin.
    use linera_views::{batch::Batch, store::WritableKeyValueStore as _, views::View as _};
    let mut view = SystemExecutionState {
        description: Some(dummy_chain_description(0)),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;
    let mut batch = Batch::new();
    view.pre_save(&mut batch)?;
    view.context().store().write_batch(batch).await?;
    view.post_save();
    let blobs = view.prepare_checkpoint(u64::MAX).await?;
    let mut txn_tracker = TransactionTracker::default();
    view.apply_checkpoint(
        PreparedCheckpoint {
            blobs,
            origin_cursors: vec![(origin_a, cursor_a), (origin_b, cursor_b)],
            outbox_block_hashes: Vec::new(),
        },
        &mut txn_tracker,
    )
    .await?;
    let outcome = txn_tracker.into_outcome()?;
    assert_eq!(outcome.outgoing_messages.len(), 2);
    for (msg, expected_destination, expected_cursor) in [
        (&outcome.outgoing_messages[0], origin_a, cursor_a),
        (&outcome.outgoing_messages[1], origin_b, cursor_b),
    ] {
        assert_eq!(msg.destination, expected_destination);
        assert_eq!(
            msg.message,
            Message::System(SystemMessage::CheckpointAck {
                latest_received_cursor: expected_cursor,
            })
        );
    }

    // Receiver side: pre-populate `unfinalized_message_blocks` with three heights
    // from origin_a (the block-end hook would normally do this), then deliver the
    // checkpoint message and verify both halves of the receive logic:
    //   - `finalized_sent_messages[origin_a]` records (cursor, cert_hash).
    //   - `unfinalized_message_blocks[origin_a]` drops heights strictly below the
    //     cursor's height and retains heights at-or-above (per-block granularity).
    let mut receiver = SystemExecutionState {
        description: Some(dummy_chain_description(3)),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;
    // cursor_a is (7, 3). We pre-seed unfinalized_message_blocks[origin_a] with
    // bundles at:
    //   * (5, 0) — fully behind the ack, dropped.
    //   * (7, 2) — same height as the ack, but lower index — dropped (the
    //     recipient has consumed past it).
    //   * (7, 3) — the ack's exact position; this bundle (and any with
    //     `cursor >= cursor_a`) is retained.
    //   * (9, 0) — newer than the ack, retained.
    receiver.system.unfinalized_message_blocks.insert(
        &origin_a,
        std::collections::BTreeSet::from([
            Cursor {
                height: BlockHeight::from(5),
                index: 0,
            },
            Cursor {
                height: BlockHeight::from(7),
                index: 2,
            },
            Cursor {
                height: BlockHeight::from(7),
                index: 3,
            },
            Cursor {
                height: BlockHeight::from(9),
                index: 0,
            },
        ]),
    )?;
    let sender_cert_hash = CryptoHash::test_hash("sender block");
    let context = MessageContext {
        chain_id: dummy_chain_description(3).id(),
        origin: origin_a,
        origin_certificate_hash: sender_cert_hash,
        origin_timestamp: Default::default(),
        is_bouncing: false,
        authenticated_owner: None,
        refund_grant_to: None,
        height: BlockHeight::from(0),
        round: Some(0),
        timestamp: Default::default(),
    };
    let produced = receiver
        .system
        .execute_message(
            context,
            SystemMessage::CheckpointAck {
                latest_received_cursor: cursor_a,
            },
        )
        .await?;
    assert!(produced.is_empty());
    assert_eq!(
        receiver
            .system
            .finalized_sent_messages
            .get(&origin_a)
            .await?,
        Some((cursor_a, sender_cert_hash))
    );
    assert_eq!(
        receiver
            .system
            .unfinalized_message_blocks
            .get(&origin_a)
            .await?,
        Some(std::collections::BTreeSet::from([
            Cursor {
                height: BlockHeight::from(7),
                index: 3,
            },
            Cursor {
                height: BlockHeight::from(9),
                index: 0,
            },
        ]))
    );

    Ok(())
}

#[tokio::test]
async fn execute_checkpoint_rejects_chain_with_published_events() -> anyhow::Result<()> {
    use linera_base::identifiers::StreamId;

    let mut view = SystemExecutionState {
        description: Some(dummy_chain_description(0)),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    view.previous_event_blocks
        .insert(&StreamId::system(b"events"), BlockHeight::from(0))?;

    let result = view.prepare_checkpoint(u64::MAX).await;
    assert!(matches!(
        result,
        Err(crate::ExecutionError::CheckpointPreconditionFailed(_))
    ));
    Ok(())
}
