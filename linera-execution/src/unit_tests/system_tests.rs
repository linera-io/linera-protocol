// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::data_types::{Blob, BlockHeight, Bytecode};
#[cfg(with_testing)]
use linera_base::vm::VmRuntime;
use linera_views::context::MemoryContext;

use super::*;
use crate::{
    test_utils::dummy_chain_description, ExecutionStateView, TestExecutionRuntimeContext,
    FLAG_ZERO_HASH,
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
        authenticated_signer: None,
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
async fn hashing_test() -> anyhow::Result<()> {
    let (mut view, _) = new_view_and_context().await;

    let zero_hash = CryptoHash::from([0u8; 32]);

    assert_ne!(view.crypto_hash_mut().await?, zero_hash);

    let mut committees = BTreeMap::new();
    committees.insert(Epoch(0), Committee::default());
    view.system.committees.set(committees.clone());
    view.system.epoch.set(Epoch(0));
    // The hash should still be nonzero before the threshold epoch.
    assert_ne!(view.crypto_hash_mut().await?, zero_hash);

    let mut committee = Committee::default();
    committee
        .policy_mut()
        .http_request_allow_list
        .insert(FLAG_ZERO_HASH.to_owned());
    committees.insert(Epoch(1), committee);
    view.system.committees.set(committees);
    view.system.epoch.set(Epoch(1));
    // Starting from this epoch, the hash should be all zeros.
    assert_eq!(view.crypto_hash_mut().await?, zero_hash);

    Ok(())
}
