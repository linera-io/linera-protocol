// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    committee::{Committee, Epoch, ValidatorName, ValidatorState},
    system::{Recipient, UserData},
    ApplicationId, Bytecode, ChainOwnership, ChannelSubscription, ExecutionStateView,
    SystemExecutionStateView, UserApplicationDescription,
};
use async_graphql::{Error, Object};
use linera_base::{
    data_types::{Amount, Timestamp},
    doc_scalar,
    identifiers::{ChainDescription, ChainId},
};
use linera_views::{common::Context, views::ViewError};
use std::collections::BTreeMap;

doc_scalar!(ApplicationId, "A unique identifier for an application");
doc_scalar!(Bytecode, "A WebAssembly module's bytecode");
doc_scalar!(ChainOwnership, "Represents the owner(s) of a chain");
doc_scalar!(
    Epoch,
    "A number identifying the configuration of the chain (aka the committee)"
);
doc_scalar!(Recipient, "The recipient of a transfer");
doc_scalar!(
    UserApplicationDescription,
    "Description of the necessary information to run a user application"
);
doc_scalar!(UserData, "Optional user message attached to a transfer");
doc_scalar!(ValidatorName, "The identity of a validator");

#[Object]
impl Committee {
    #[graphql(derived(name = "validators"))]
    async fn _validators(&self) -> &BTreeMap<ValidatorName, ValidatorState> {
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
}

#[async_graphql::Object]
impl<C: Send + Sync + Context> ExecutionStateView<C>
where
    ViewError: From<C::Error>,
{
    #[graphql(derived(name = "system"))]
    async fn _system(&self) -> &SystemExecutionStateView<C> {
        &self.system
    }
}

#[async_graphql::Object]
impl<C: Send + Sync + Context> SystemExecutionStateView<C>
where
    ViewError: From<C::Error>,
{
    #[graphql(derived(name = "description"))]
    async fn _description(&self) -> &Option<ChainDescription> {
        self.description.get()
    }

    #[graphql(derived(name = "epoch"))]
    async fn _epoch(&self) -> &Option<Epoch> {
        self.epoch.get()
    }

    #[graphql(derived(name = "admin_id"))]
    async fn _admin_id(&self) -> &Option<ChainId> {
        self.admin_id.get()
    }

    #[graphql(derived(name = "subscription"))]
    async fn _subscriptions(&self) -> Result<Vec<ChannelSubscription>, Error> {
        Ok(self.subscriptions.indices().await?)
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

    #[graphql(derived(name = "timestamp"))]
    async fn _timestamp(&self) -> &Timestamp {
        self.timestamp.get()
    }
}
