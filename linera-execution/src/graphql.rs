use crate::{
    system::Balance, ChainOwnership, ChannelName, ExecutionStateView, SystemExecutionStateView,
};
use async_graphql::scalar;
use linera_base::{
    committee::Committee,
    data_types::{ChainDescription, ChainId, Epoch},
};
use linera_views::common::Context;
use std::collections::BTreeMap;

scalar!(Balance);
scalar!(ChainOwnership);
scalar!(ChannelName);

#[async_graphql::Object]
impl<C: Send + Sync + Context> ExecutionStateView<C> {
    async fn system(&self) -> &SystemExecutionStateView<C> {
        &self.system
    }
}

#[async_graphql::Object]
impl<C: Send + Sync + Context> SystemExecutionStateView<C> {
    async fn description(&self) -> &Option<ChainDescription> {
        self.description.get()
    }

    async fn epoch(&self) -> &Option<Epoch> {
        self.epoch.get()
    }

    async fn admin_id(&self) -> &Option<ChainId> {
        self.admin_id.get()
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
}
