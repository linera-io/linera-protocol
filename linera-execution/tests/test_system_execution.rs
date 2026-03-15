// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use linera_base::{
    crypto::AccountSecretKey,
    data_types::{Amount, BlockHeight, TimeDelta, Timestamp},
    identifiers::{Account, AccountOwner},
    ownership::ChainOwnership,
};
use linera_execution::{
    test_utils::{
        dummy_chain_description, dummy_chain_description_with_ownership_and_balance,
        SystemExecutionState,
    },
    ExecutionError, ExecutionStateActor, Message, MessageContext, Operation, OperationContext,
    Query, QueryContext, QueryOutcome, QueryResponse, ResourceController, SystemMessage,
    SystemOperation, SystemQuery, SystemResponse, TransactionTracker,
};

#[tokio::test]
async fn test_simple_system_operation() -> anyhow::Result<()> {
    let owner_key_pair = AccountSecretKey::generate();
    let owner = AccountOwner::from(owner_key_pair.public());
    let ownership = ChainOwnership {
        super_owners: [owner].into_iter().collect(),
        ..ChainOwnership::default()
    };
    let balance = Amount::from_tokens(4);
    let description =
        dummy_chain_description_with_ownership_and_balance(0, ownership.clone(), balance);
    let chain_id = description.id();
    let state = SystemExecutionState {
        description: Some(description),
        balance,
        ownership,
        ..SystemExecutionState::default()
    };
    let mut view = state.into_view().await;
    let recipient = Account::burn_address(chain_id);
    let operation = SystemOperation::Transfer {
        owner: AccountOwner::CHAIN,
        amount: Amount::from_tokens(4),
        recipient,
    };
    let context = OperationContext {
        chain_id,
        height: BlockHeight(0),
        round: Some(0),
        authenticated_owner: Some(owner),
        timestamp: Default::default(),
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying(Vec::new());
    ExecutionStateActor::new(&mut view, &mut txn_tracker, &mut controller)
        .execute_operation(context, Operation::system(operation))
        .await?;
    assert_eq!(view.system.balance.get(), &Amount::ZERO);
    let txn_outcome = txn_tracker.into_outcome().unwrap();
    assert!(txn_outcome.outgoing_messages.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_simple_system_message() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    let description = dummy_chain_description(0);
    let chain_id = description.id();
    state.description = Some(description);
    let mut view = state.into_view().await;
    let message = SystemMessage::Credit {
        amount: Amount::from_tokens(4),
        target: AccountOwner::CHAIN,
        source: AccountOwner::CHAIN,
    };
    let context = MessageContext {
        chain_id,
        origin: chain_id,
        is_bouncing: false,
        height: BlockHeight(0),
        round: Some(0),
        authenticated_owner: None,
        refund_grant_to: None,
        timestamp: Default::default(),
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying(Vec::new());
    ExecutionStateActor::new(&mut view, &mut txn_tracker, &mut controller)
        .execute_message(context, Message::System(message), None)
        .await?;
    assert_eq!(view.system.balance.get(), &Amount::from_tokens(4));
    let txn_outcome = txn_tracker.into_outcome().unwrap();
    assert!(txn_outcome.outgoing_messages.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_simple_system_query() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    let description = dummy_chain_description(0);
    let chain_id = description.id();
    state.description = Some(description);
    state.balance = Amount::from_tokens(4);
    let mut view = state.into_view().await;
    let context = QueryContext {
        chain_id,
        next_block_height: BlockHeight(0),
        local_time: Timestamp::from(0),
    };
    let QueryOutcome {
        response,
        operations,
    } = view
        .query_application(context, Query::System(SystemQuery), None)
        .await
        .unwrap();
    assert_eq!(
        response,
        QueryResponse::System(SystemResponse {
            chain_id,
            balance: Amount::from_tokens(4)
        })
    );
    assert!(operations.is_empty());
    Ok(())
}

/// A super owner can change super owners.
#[tokio::test]
async fn test_change_super_owners_by_super_owner() -> anyhow::Result<()> {
    let super_owner_key = AccountSecretKey::generate();
    let super_owner = AccountOwner::from(super_owner_key.public());
    let new_super_owner = AccountOwner::from(AccountSecretKey::generate().public());
    let ownership = ChainOwnership {
        super_owners: [super_owner].into_iter().collect(),
        ..ChainOwnership::default()
    };
    let balance = Amount::from_tokens(4);
    let description =
        dummy_chain_description_with_ownership_and_balance(0, ownership.clone(), balance);
    let chain_id = description.id();
    let state = SystemExecutionState {
        description: Some(description),
        balance,
        ownership,
        ..SystemExecutionState::default()
    };
    let mut view = state.into_view().await;
    let operation = SystemOperation::ChangeSuperOwners {
        super_owners: vec![new_super_owner],
        fast_round_duration: Some(TimeDelta::from_secs(5)),
    };
    let context = OperationContext {
        chain_id,
        height: BlockHeight(0),
        round: Some(0),
        authenticated_owner: Some(super_owner),
        timestamp: Default::default(),
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying(Vec::new());
    ExecutionStateActor::new(&mut view, &mut txn_tracker, &mut controller)
        .execute_operation(context, Operation::system(operation))
        .await?;
    let new_ownership = view.system.ownership.get();
    assert!(new_ownership.super_owners.contains(&new_super_owner));
    assert!(!new_ownership.super_owners.contains(&super_owner));
    assert_eq!(
        new_ownership.timeout_config.fast_round_duration,
        Some(TimeDelta::from_secs(5))
    );
    Ok(())
}

/// A regular owner cannot change super owners.
#[tokio::test]
async fn test_change_super_owners_by_regular_owner_fails() -> anyhow::Result<()> {
    let super_owner = AccountOwner::from(AccountSecretKey::generate().public());
    let regular_owner = AccountOwner::from(AccountSecretKey::generate().public());
    let ownership = ChainOwnership {
        super_owners: [super_owner].into_iter().collect(),
        owners: [(regular_owner, 100)].into_iter().collect(),
        ..ChainOwnership::default()
    };
    let balance = Amount::from_tokens(4);
    let description =
        dummy_chain_description_with_ownership_and_balance(0, ownership.clone(), balance);
    let chain_id = description.id();
    let state = SystemExecutionState {
        description: Some(description),
        balance,
        ownership,
        ..SystemExecutionState::default()
    };
    let mut view = state.into_view().await;
    let operation = SystemOperation::ChangeSuperOwners {
        super_owners: vec![regular_owner],
        fast_round_duration: None,
    };
    let context = OperationContext {
        chain_id,
        height: BlockHeight(0),
        round: Some(0),
        authenticated_owner: Some(regular_owner),
        timestamp: Default::default(),
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying(Vec::new());
    let result = ExecutionStateActor::new(&mut view, &mut txn_tracker, &mut controller)
        .execute_operation(context, Operation::system(operation))
        .await;
    assert!(matches!(
        result,
        Err(ExecutionError::UnauthorizedChangeSuperOwners)
    ));
    Ok(())
}

/// A chain without super owners cannot gain super owners.
#[tokio::test]
async fn test_change_super_owners_on_chain_without_super_owners_fails() -> anyhow::Result<()> {
    let regular_owner = AccountOwner::from(AccountSecretKey::generate().public());
    let ownership = ChainOwnership {
        owners: [(regular_owner, 100)].into_iter().collect(),
        ..ChainOwnership::default()
    };
    let balance = Amount::from_tokens(4);
    let description =
        dummy_chain_description_with_ownership_and_balance(0, ownership.clone(), balance);
    let chain_id = description.id();
    let state = SystemExecutionState {
        description: Some(description),
        balance,
        ownership,
        ..SystemExecutionState::default()
    };
    let mut view = state.into_view().await;
    let new_super_owner = AccountOwner::from(AccountSecretKey::generate().public());
    let operation = SystemOperation::ChangeSuperOwners {
        super_owners: vec![new_super_owner],
        fast_round_duration: None,
    };
    let context = OperationContext {
        chain_id,
        height: BlockHeight(0),
        round: Some(0),
        authenticated_owner: Some(regular_owner),
        timestamp: Default::default(),
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying(Vec::new());
    let result = ExecutionStateActor::new(&mut view, &mut txn_tracker, &mut controller)
        .execute_operation(context, Operation::system(operation))
        .await;
    assert!(matches!(
        result,
        Err(ExecutionError::UnauthorizedChangeSuperOwners)
    ));
    Ok(())
}

/// ChangeSuperOwners fails without an authenticated owner.
#[tokio::test]
async fn test_change_super_owners_without_authenticated_owner_fails() -> anyhow::Result<()> {
    let super_owner = AccountOwner::from(AccountSecretKey::generate().public());
    let ownership = ChainOwnership {
        super_owners: [super_owner].into_iter().collect(),
        ..ChainOwnership::default()
    };
    let balance = Amount::from_tokens(4);
    let description =
        dummy_chain_description_with_ownership_and_balance(0, ownership.clone(), balance);
    let chain_id = description.id();
    let state = SystemExecutionState {
        description: Some(description),
        balance,
        ownership,
        ..SystemExecutionState::default()
    };
    let mut view = state.into_view().await;
    let operation = SystemOperation::ChangeSuperOwners {
        super_owners: vec![super_owner],
        fast_round_duration: None,
    };
    let context = OperationContext {
        chain_id,
        height: BlockHeight(0),
        round: Some(0),
        authenticated_owner: None,
        timestamp: Default::default(),
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying(Vec::new());
    let result = ExecutionStateActor::new(&mut view, &mut txn_tracker, &mut controller)
        .execute_operation(context, Operation::system(operation))
        .await;
    assert!(matches!(
        result,
        Err(ExecutionError::UnauthorizedChangeSuperOwners)
    ));
    Ok(())
}

/// A regular owner can change regular owners via ChangeOwners.
#[tokio::test]
async fn test_change_owners_preserves_super_owners() -> anyhow::Result<()> {
    let super_owner = AccountOwner::from(AccountSecretKey::generate().public());
    let regular_owner = AccountOwner::from(AccountSecretKey::generate().public());
    let new_owner = AccountOwner::from(AccountSecretKey::generate().public());
    let ownership = ChainOwnership {
        super_owners: [super_owner].into_iter().collect(),
        owners: [(regular_owner, 100)].into_iter().collect(),
        ..ChainOwnership::default()
    };
    let balance = Amount::from_tokens(4);
    let description =
        dummy_chain_description_with_ownership_and_balance(0, ownership.clone(), balance);
    let chain_id = description.id();
    let state = SystemExecutionState {
        description: Some(description),
        balance,
        ownership,
        ..SystemExecutionState::default()
    };
    let mut view = state.into_view().await;
    let tc = linera_base::ownership::TimeoutConfig::default();
    let operation = SystemOperation::ChangeOwners {
        owners: vec![(new_owner, 100)],
        first_leader: None,
        multi_leader_rounds: 2,
        open_multi_leader_rounds: false,
        base_timeout: tc.base_timeout,
        timeout_increment: tc.timeout_increment,
        fallback_duration: tc.fallback_duration,
    };
    let context = OperationContext {
        chain_id,
        height: BlockHeight(0),
        round: Some(0),
        authenticated_owner: Some(regular_owner),
        timestamp: Default::default(),
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying(Vec::new());
    ExecutionStateActor::new(&mut view, &mut txn_tracker, &mut controller)
        .execute_operation(context, Operation::system(operation))
        .await?;
    let new_ownership = view.system.ownership.get();
    // Super owners are preserved by ChangeOwners.
    assert!(new_ownership.super_owners.contains(&super_owner));
    // Regular owners were changed.
    assert!(new_ownership.owners.contains_key(&new_owner));
    assert!(!new_ownership.owners.contains_key(&regular_owner));
    Ok(())
}
