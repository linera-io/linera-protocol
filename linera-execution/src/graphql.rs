// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{Amount, ChainDescription, Epoch, Timestamp},
    doc_scalar,
    identifiers::{AccountOwner, ChainId},
    ownership::ChainOwnership,
};
use linera_views::{context::Context, map_view::MapView};

use crate::{
    committee::{Committee, ValidatorState},
    policy::ResourceControlPolicy,
    system::UserData,
    ExecutionStateView, SystemExecutionStateView,
};

doc_scalar!(UserData, "Optional user message attached to a transfer");

async_graphql::scalar!(
    ResourceControlPolicy,
    "ResourceControlPolicyScalar",
    "A collection of prices and limits associated with block execution"
);

#[async_graphql::Object(cache_control(no_cache))]
impl Committee {
    #[graphql(derived(name = "validators"))]
    async fn _validators(&self) -> &BTreeMap<ValidatorPublicKey, ValidatorState> {
        self.validators()
    }

    #[graphql(derived(name = "total_votes"))]
    async fn _total_votes(&self) -> u64 {
        self.total_votes()
    }

    #[graphql(derived(name = "quorum_threshold"))]
    async fn _quorum_threshold(&self) -> u64 {
        self.quorum_threshold()
    }

    #[graphql(derived(name = "validity_threshold"))]
    async fn _validity_threshold(&self) -> u64 {
        self.validity_threshold()
    }

    #[graphql(derived(name = "policy"))]
    async fn _policy(&self) -> &ResourceControlPolicy {
        self.policy()
    }
}

#[async_graphql::Object(cache_control(no_cache))]
impl<C: Send + Sync + Context> ExecutionStateView<C> {
    #[graphql(derived(name = "system"))]
    async fn _system(&self) -> &SystemExecutionStateView<C> {
        &self.system
    }
}

#[async_graphql::Object(cache_control(no_cache))]
impl<C: Send + Sync + Context> SystemExecutionStateView<C> {
    #[graphql(derived(name = "description"))]
    async fn _description(&self) -> &Option<ChainDescription> {
        self.description.get()
    }

    #[graphql(derived(name = "epoch"))]
    async fn _epoch(&self) -> &Epoch {
        self.epoch.get()
    }

    #[graphql(derived(name = "admin_chain_id"))]
    async fn _admin_chain_id(&self) -> &Option<ChainId> {
        self.admin_chain_id.get()
    }

    #[graphql(derived(name = "committees"))]
    async fn _committees(&self) -> &BTreeMap<Epoch, Committee> {
        self.committees.get()
    }

    #[graphql(derived(name = "ownership"))]
    async fn _ownership(&self) -> &ChainOwnership {
        self.ownership.get()
    }

    #[graphql(derived(name = "balance"))]
    async fn _balance(&self) -> &Amount {
        self.balance.get()
    }

    #[graphql(derived(name = "balances"))]
    async fn _balances(&self) -> &MapView<C, AccountOwner, Amount> {
        &self.balances
    }

    #[graphql(derived(name = "timestamp"))]
    async fn _timestamp(&self) -> &Timestamp {
        self.timestamp.get()
    }
}
