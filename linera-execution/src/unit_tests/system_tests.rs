// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::data_types::{Blob, BlockHeight, Bytecode, Timestamp};
#[cfg(with_testing)]
use linera_base::vm::VmRuntime;
use linera_views::context::MemoryContext;

use super::*;
use crate::{
    test_utils::dummy_chain_description, ExecutionStateView, TestExecutionRuntimeContext,
    FLAG_HISTORICAL_HASH, FLAG_HISTORICAL_HASH_SHADOW, FLAG_ZERO_HASH,
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

/// Installs a single committee at epoch 1 (the epoch `new_view_and_context` uses) whose content
/// policy carries `flag`, so that `crypto_hash_mut` selects the corresponding hashing mode.
fn install_committee_with_flag(
    view: &mut ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>,
    flag: &str,
) {
    let mut committee = Committee::default();
    committee
        .policy_mut()
        .http_request_allow_list
        .insert(flag.to_owned());
    let mut committees = BTreeMap::new();
    committees.insert(Epoch(1), committee);
    view.system.committees.set(committees);
    view.system.epoch.set(Epoch(1));
}

#[tokio::test]
async fn historical_hashing_enforce_seeds_then_chains() -> anyhow::Result<()> {
    let zero_hash = CryptoHash::from([0u8; 32]);
    let (mut view, _) = new_view_and_context().await;
    install_committee_with_flag(&mut view, FLAG_HISTORICAL_HASH);

    // The first block seeds the rolling hash: a non-zero hash is reported and the register is
    // populated.
    assert!(view.historical_hash.get().is_none());
    let seed = view.crypto_hash_mut().await?;
    assert_ne!(seed, zero_hash);
    let seed_raw = *view.historical_hash.get();
    assert!(seed_raw.is_some());

    // A later block with a content change extends the chain: a different, still non-zero hash,
    // and the stored rolling hash advances.
    view.system.timestamp.set(Timestamp::from(42));
    let chained = view.crypto_hash_mut().await?;
    assert_ne!(chained, zero_hash);
    assert_ne!(chained, seed);
    assert_ne!(*view.historical_hash.get(), seed_raw);

    Ok(())
}

#[tokio::test]
async fn historical_hashing_shadow_reports_zero_but_advances() -> anyhow::Result<()> {
    let zero_hash = CryptoHash::from([0u8; 32]);
    let (mut view, _) = new_view_and_context().await;
    install_committee_with_flag(&mut view, FLAG_HISTORICAL_HASH_SHADOW);

    // Shadow mode reports an all-zeros hash to consensus...
    let reported = view.crypto_hash_mut().await?;
    assert_eq!(reported, zero_hash);
    // ...yet still computes and stores the rolling hash for cross-validator comparison.
    let seed_raw = *view.historical_hash.get();
    assert!(seed_raw.is_some());

    view.system.timestamp.set(Timestamp::from(42));
    assert_eq!(view.crypto_hash_mut().await?, zero_hash);
    assert_ne!(*view.historical_hash.get(), seed_raw);

    Ok(())
}

#[tokio::test]
async fn historical_hashing_is_deterministic() -> anyhow::Result<()> {
    // Two runs with identical state and identical operations must agree; a different operation
    // must diverge.
    async fn run(timestamp: u64) -> anyhow::Result<(CryptoHash, CryptoHash)> {
        let (mut view, _) = new_view_and_context().await;
        install_committee_with_flag(&mut view, FLAG_HISTORICAL_HASH);
        let seed = view.crypto_hash_mut().await?;
        view.system.timestamp.set(Timestamp::from(timestamp));
        let chained = view.crypto_hash_mut().await?;
        Ok((seed, chained))
    }

    let (seed_a, chained_a) = Box::pin(run(42)).await?;
    let (seed_b, chained_b) = Box::pin(run(42)).await?;
    assert_eq!(seed_a, seed_b);
    assert_eq!(chained_a, chained_b);

    let (seed_c, chained_c) = Box::pin(run(99)).await?;
    assert_eq!(seed_a, seed_c); // seed is computed before the differing mutation
    assert_ne!(chained_a, chained_c); // the differing mutation changes the chained hash

    Ok(())
}

#[tokio::test]
async fn historical_hashing_persists_and_chains_across_reload() -> anyhow::Result<()> {
    use linera_views::{
        batch::Batch, context::Context as _, store::WritableKeyValueStore as _, views::View as _,
    };

    // Flushes a view's pending changes to its (shared) store and clears the in-memory dirty state,
    // exactly as the core save lifecycle does between blocks.
    async fn save(view: &mut ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>) {
        let mut batch = Batch::new();
        view.pre_save(&mut batch).unwrap();
        view.context().store().write_batch(batch).await.unwrap();
        view.post_save();
    }

    // The committee lives in a `HashedLazyRegisterView`, which the ad-hoc `save` helper above does
    // not round-trip (production persists the whole `RootView`). So the enforce flag is
    // re-installed in memory after every reload; this is a test-harness concern, not a property of
    // the historical hashing itself, whose rolling hash lives in a plain `RegisterView`.
    let zero_hash = CryptoHash::from([0u8; 32]);

    // Persist a chain's pre-activation content. The `historical_hash` register is never written,
    // mirroring a chain created before activation.
    let (mut view, _) = new_view_and_context().await;
    let context = view.context().clone();
    save(&mut view).await;

    // Reload as a pre-activation chain: the appended field is absent in storage, so it loads as
    // `None` without any migration (format compatibility).
    let mut view = ExecutionStateView::load(context.clone()).await?;
    assert!(view.historical_hash.get().is_none());

    // Block 1 — seed, then persist and reload. The seeded rolling hash must survive the round-trip.
    install_committee_with_flag(&mut view, FLAG_HISTORICAL_HASH);
    let seed = view.crypto_hash_mut().await?;
    assert_ne!(seed, zero_hash);
    let seed_raw = *view.historical_hash.get();
    assert!(seed_raw.is_some());
    save(&mut view).await;
    let mut view = ExecutionStateView::load(context.clone()).await?;
    assert_eq!(*view.historical_hash.get(), seed_raw); // rolling hash round-trips

    // Block 2 — mutate and chain *from the reloaded* stored hash. The result differs from the seed
    // (the chain advanced) and persists across the next reload.
    install_committee_with_flag(&mut view, FLAG_HISTORICAL_HASH);
    view.system.timestamp.set(Timestamp::from(42));
    let chained = view.crypto_hash_mut().await?;
    assert_ne!(chained, seed);
    assert_ne!(chained, zero_hash);
    let chained_raw = *view.historical_hash.get();
    save(&mut view).await;
    let view = ExecutionStateView::load(context.clone()).await?;
    assert_eq!(*view.historical_hash.get(), chained_raw);

    Ok(())
}
