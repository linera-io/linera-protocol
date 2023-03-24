// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    committee::{Committee, Epoch, ValidatorName, ValidatorState},
    system::{Recipient, UserData},
    ApplicationId, Bytecode, ChainOwnership, ChannelId, ExecutionStateView,
    SystemExecutionStateView, UserApplicationDescription,
};
use async_graphql::{scalar, Error, Object};
use linera_base::{
    data_types::{Balance, Timestamp},
    identifiers::{ChainDescription, ChainId},
};
use linera_views::{common::Context, views::ViewError};
use std::collections::BTreeMap;

scalar!(ApplicationId);
scalar!(Bytecode);

scalar!(ChainOwnership);
scalar!(Epoch);
scalar!(Recipient);
scalar!(UserApplicationDescription);
scalar!(UserData);
scalar!(ValidatorName);

#[Object]
impl Committee {
    #[graphql(derived(name = "validators"))]
    async fn _validators(&self) -> &BTreeMap<ValidatorName, ValidatorState> {
        &self.validators
    }

    #[graphql(derived(name = "total_votes"))]
    async fn _total_votes(&self) -> u64 {
        self.total_votes
    }

    #[graphql(derived(name = "quorum_threshold"))]
    async fn _quorum_threshold(&self) -> u64 {
        self.quorum_threshold
    }

    #[graphql(derived(name = "validity_threshold"))]
    async fn _validity_threshold(&self) -> u64 {
        self.validity_threshold
    }
}

#[async_graphql::Object]
impl<C: Send + Sync + Context> ExecutionStateView<C>
where
    ViewError: From<C::Error>,
{
    async fn system(&self) -> &SystemExecutionStateView<C> {
        &self.system
    }
}

#[async_graphql::Object]
impl<C: Send + Sync + Context> SystemExecutionStateView<C>
where
    ViewError: From<C::Error>,
{
    async fn description(&self) -> &Option<ChainDescription> {
        self.description.get()
    }

    async fn epoch(&self) -> &Option<Epoch> {
        self.epoch.get()
    }

    async fn admin_id(&self) -> &Option<ChainId> {
        self.admin_id.get()
    }

    async fn subscriptions(&self) -> Result<Vec<ChannelId>, Error> {
        Ok(self.subscriptions.indices().await?)
    }

    async fn committees(&self) -> &BTreeMap<Epoch, Committee> {
        self.committees.get()
    }

    async fn ownership(&self) -> &ChainOwnership {
        self.ownership.get()
    }

    async fn balance(&self) -> &Balance {
        self.balance.get()
    }

    async fn timestamp(&self) -> &Timestamp {
        self.timestamp.get()
    }
}
