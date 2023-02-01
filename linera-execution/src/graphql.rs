use crate::{
    system::Balance, ApplicationId, ChainOwnership, ChannelId, ChannelName, ExecutionStateView,
    SystemExecutionStateView,
};
use async_graphql::{scalar, Error};
use linera_base::{
    committee::Committee,
    data_types::{ChainDescription, ChainId, Epoch, Timestamp},
};
use linera_views::{common::Context, views::ViewError};
use std::collections::BTreeMap;

scalar!(ApplicationId);
scalar!(Balance);
scalar!(ChainOwnership);
scalar!(ChannelName);

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
