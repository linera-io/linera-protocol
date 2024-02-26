// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    applications::ApplicationRegistry,
    committee::{Committee, Epoch},
    execution::UserAction,
    system::SystemChannel,
    ChannelSubscription, ExecutionError, ExecutionRuntimeConfig, ExecutionRuntimeContext,
    ExecutionStateView, OperationContext, ResourceControlPolicy, ResourceController,
    ResourceTracker, TestExecutionRuntimeContext, UserApplicationDescription, UserContractCode,
};
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, Timestamp},
    identifiers::{ApplicationId, ChainDescription, ChainId, Owner},
    ownership::ChainOwnership,
};
use linera_views::{
    common::Context,
    memory::{MemoryContext, TEST_MEMORY_MAX_STREAM_QUERIES},
    views::{CryptoHashView, View, ViewError},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// A system execution state, not represented as a view but as a simple struct.
#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct SystemExecutionState {
    pub description: Option<ChainDescription>,
    pub epoch: Option<Epoch>,
    pub admin_id: Option<ChainId>,
    pub subscriptions: BTreeSet<ChannelSubscription>,
    pub committees: BTreeMap<Epoch, Committee>,
    pub ownership: ChainOwnership,
    pub balance: Amount,
    pub balances: BTreeMap<Owner, Amount>,
    pub timestamp: Timestamp,
    pub registry: ApplicationRegistry,
    pub closed: bool,
    pub authorized_applications: Option<BTreeSet<ApplicationId>>,
}

impl SystemExecutionState {
    pub fn new(epoch: Epoch, description: ChainDescription, admin_id: impl Into<ChainId>) -> Self {
        let admin_id = admin_id.into();
        let subscriptions = if ChainId::from(description) == admin_id {
            BTreeSet::new()
        } else {
            BTreeSet::from([ChannelSubscription {
                chain_id: admin_id,
                name: SystemChannel::Admin.name(),
            }])
        };
        SystemExecutionState {
            epoch: Some(epoch),
            description: Some(description),
            admin_id: Some(admin_id),
            subscriptions,
            ..SystemExecutionState::default()
        }
    }

    pub async fn into_hash(self) -> CryptoHash {
        ExecutionStateView::from_system_state(self, ExecutionRuntimeConfig::default())
            .await
            .crypto_hash()
            .await
            .expect("hashing from memory should not fail")
    }
}

impl ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>
where
    MemoryContext<TestExecutionRuntimeContext>: Context + Clone + Send + Sync + 'static,
    ViewError:
        From<<MemoryContext<TestExecutionRuntimeContext> as linera_views::common::Context>::Error>,
{
    /// Creates an in-memory view where the system state is set. This is used notably to
    /// generate state hashes in tests.
    pub async fn from_system_state(
        state: SystemExecutionState,
        execution_runtime_config: ExecutionRuntimeConfig,
    ) -> Self {
        // Destructure, to make sure we don't miss any fields.
        let SystemExecutionState {
            description,
            epoch,
            admin_id,
            subscriptions,
            committees,
            ownership,
            balance,
            balances,
            timestamp,
            registry,
            closed,
            authorized_applications,
        } = state;
        let extra = TestExecutionRuntimeContext::new(
            description.expect("Chain description should be set").into(),
            execution_runtime_config,
        );
        let context = MemoryContext::new(TEST_MEMORY_MAX_STREAM_QUERIES, extra);
        let mut view = Self::load(context)
            .await
            .expect("Loading from memory should work");
        view.system.description.set(description);
        view.system.epoch.set(epoch);
        view.system.admin_id.set(admin_id);
        for subscription in subscriptions {
            view.system
                .subscriptions
                .insert(&subscription)
                .expect("serialization of subscription should not fail");
        }
        view.system.committees.set(committees);
        view.system.ownership.set(ownership);
        view.system.balance.set(balance);
        for (owner, balance) in balances {
            view.system
                .balances
                .insert(&owner, balance)
                .expect("insertion of balances should not fail");
        }
        view.system.timestamp.set(timestamp);
        view.system
            .registry
            .import(registry)
            .expect("serialization of registry components should not fail");
        view.system.closed.set(closed);
        view.system
            .authorized_applications
            .set(authorized_applications);
        view
    }
}
