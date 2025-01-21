// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    data_types::{Blob, BlockHeight, Bytecode},
    identifiers::ApplicationId,
};
use linera_views::context::MemoryContext;

use super::*;
use crate::{ExecutionOutcome, ExecutionStateView, TestExecutionRuntimeContext};

/// Returns an execution state view and a matching operation context, for epoch 1, with root
/// chain 0 as the admin ID and one empty committee.
async fn new_view_and_context() -> (
    ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>,
    OperationContext,
) {
    let description = ChainDescription::Root(5);
    let context = OperationContext {
        chain_id: ChainId::from(description),
        authenticated_signer: None,
        authenticated_caller_id: None,
        height: BlockHeight::from(7),
        index: Some(2),
    };
    let state = SystemExecutionState {
        description: Some(description),
        epoch: Some(Epoch(1)),
        admin_id: Some(ChainId::root(0)),
        committees: BTreeMap::new(),
        ..SystemExecutionState::default()
    };
    let view = state.into_view().await;
    (view, context)
}

#[tokio::test]
async fn application_message_index() -> anyhow::Result<()> {
    let (mut view, context) = new_view_and_context().await;
    let contract = Bytecode::new(b"contract".into());
    let service = Bytecode::new(b"service".into());
    let contract_blob = Blob::new_contract_bytecode(contract.compress());
    let service_blob = Blob::new_service_bytecode(service.compress());
    let bytecode_id = BytecodeId::new(contract_blob.id().hash, service_blob.id().hash);

    let operation = SystemOperation::CreateApplication {
        bytecode_id,
        parameters: vec![],
        instantiation_argument: vec![],
        required_application_ids: vec![],
    };
    let mut txn_tracker = TransactionTracker::default();
    view.context()
        .extra()
        .add_blobs([contract_blob, service_blob])
        .await?;
    let new_application = view
        .system
        .execute_operation(context, operation, &mut txn_tracker)
        .await?;
    let [ExecutionOutcome::System(result)] = &txn_tracker.destructure()?.0[..] else {
        panic!("Unexpected outcome");
    };
    assert_eq!(
        result.messages[CREATE_APPLICATION_MESSAGE_INDEX as usize].message,
        SystemMessage::ApplicationCreated
    );
    let creation = MessageId {
        chain_id: context.chain_id,
        height: context.height,
        index: CREATE_APPLICATION_MESSAGE_INDEX,
    };
    let id = ApplicationId {
        bytecode_id,
        creation,
    };
    assert_eq!(new_application, Some((id, vec![])));

    Ok(())
}

#[tokio::test]
async fn open_chain_message_index() {
    let (mut view, context) = new_view_and_context().await;
    let epoch = view.system.epoch.get().unwrap();
    let admin_id = view.system.admin_id.get().unwrap();
    let committees = view.system.committees.get().clone();
    let owner = linera_base::crypto::PublicKey::test_key(0).into();
    let ownership = ChainOwnership::single(owner);
    let config = OpenChainConfig {
        ownership,
        committees,
        epoch,
        admin_id,
        balance: Amount::ZERO,
        application_permissions: Default::default(),
    };
    let mut txn_tracker = TransactionTracker::default();
    let operation = SystemOperation::OpenChain(config.clone());
    let new_application = view
        .system
        .execute_operation(context, operation, &mut txn_tracker)
        .await
        .unwrap();
    assert_eq!(new_application, None);
    let [ExecutionOutcome::System(result)] = &txn_tracker.destructure().unwrap().0[..] else {
        panic!("Unexpected outcome");
    };
    assert_eq!(
        result.messages[OPEN_CHAIN_MESSAGE_INDEX as usize].message,
        SystemMessage::OpenChain(config)
    );
}

/// Tests if an account is removed from storage if it is drained.
#[tokio::test]
async fn empty_accounts_are_removed() -> anyhow::Result<()> {
    let owner = AccountOwner::User(Owner(CryptoHash::test_hash("account owner")));
    let amount = Amount::from_tokens(99);

    let mut view = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        balances: BTreeMap::from([(owner, amount)]),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    view.system.debit(Some(&owner), amount).await?;

    assert!(view.system.balances.indices().await?.is_empty());

    Ok(())
}
