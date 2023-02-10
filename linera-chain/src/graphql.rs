use crate::{
    chain::{ChannelStateView, CommunicationStateView},
    data_types::{Event, Medium, Origin, Target},
    inbox::InboxStateView,
    outbox::OutboxStateView,
    ChainManager,
};
use async_graphql::{scalar, Error, Object};
use linera_base::data_types::ChainId;
use linera_execution::{ApplicationId, ChannelName};
use linera_views::{collection_view::ReadGuardedView, common::Context, views::ViewError};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

scalar!(ChainManager);
scalar!(Event);
scalar!(Medium);
scalar!(Range);

#[derive(Serialize, Deserialize)]
pub struct Range {
    start: usize,
    end: usize,
}

impl From<Range> for std::ops::Range<usize> {
    fn from(range: Range) -> Self {
        std::ops::Range {
            start: range.start,
            end: range.end,
        }
    }
}

struct CommunicationStateElement<'a, C>
where
    C: Sync + Send + Context + 'static,
    ViewError: From<C::Error>,
{
    application_id: ApplicationId,
    guard: ReadGuardedView<'a, CommunicationStateView<C>>,
}

#[Object]
impl<'a, C> CommunicationStateElement<'a, C>
where
    C: Sync + Send + Context + 'static + Clone,
    ViewError: From<C::Error>,
{
    async fn application_id(&self) -> &ApplicationId {
        &self.application_id
    }

    async fn communication_state_view(&self) -> &CommunicationStateView<C> {
        self.guard.deref()
    }
}

#[Object]
impl<C: Sync + Send + Context + Clone + 'static> CommunicationStateView<C>
where
    ViewError: From<C::Error>,
{
    async fn inbox(
        &self,
        chain_id: ChainId,
        channel_name: Option<ChannelName>,
    ) -> Result<InboxStateElement<C>, Error> {
        let origin = match channel_name {
            None => Origin::chain(chain_id),
            Some(channel_name) => Origin::channel(chain_id, channel_name),
        };
        Ok(InboxStateElement {
            origin: origin.clone(),
            guard: self.inboxes.try_load_entry(origin).await?,
        })
    }

    async fn inbox_indices(&self) -> Result<Vec<Origin>, Error> {
        Ok(self.inboxes.indices().await?)
    }

    async fn outbox(
        &self,
        chain_id: ChainId,
        channel_name: Option<ChannelName>,
    ) -> Result<OutboxStateElement<C>, Error> {
        let target = match channel_name {
            None => Target::chain(chain_id),
            Some(channel_name) => Target::channel(chain_id, channel_name),
        };
        Ok(OutboxStateElement {
            target: target.clone(),
            guard: self.outboxes.try_load_entry(target).await?,
        })
    }

    async fn outbox_indices(&self) -> Result<Vec<Target>, Error> {
        Ok(self.outboxes.indices().await?)
    }

    async fn channel(&self, channel_name: ChannelName) -> Result<ChannelStateEntry<C>, Error> {
        Ok(ChannelStateEntry {
            channel_name: channel_name.clone(),
            guard: self.channels.try_load_entry(channel_name).await?,
        })
    }

    async fn channel_indices(&self) -> Result<Vec<ChannelName>, Error> {
        Ok(self.channels.indices().await?)
    }
}

struct InboxStateElement<'a, C>
where
    C: Sync + Send + Context + 'static,
    ViewError: From<C::Error>,
{
    origin: Origin,
    guard: ReadGuardedView<'a, InboxStateView<C>>,
}

#[Object]
impl<'a, C> InboxStateElement<'a, C>
where
    C: Sync + Send + Context + 'static + Clone,
    ViewError: From<C::Error>,
{
    async fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn inbox_state_view(&self) -> &InboxStateView<C> {
        self.guard.deref()
    }
}

struct OutboxStateElement<'a, C>
where
    C: Sync + Send + Context + 'static,
    ViewError: From<C::Error>,
{
    target: Target,
    guard: ReadGuardedView<'a, OutboxStateView<C>>,
}

#[Object]
impl<'a, C> OutboxStateElement<'a, C>
where
    C: Sync + Send + Context + 'static + Clone,
    ViewError: From<C::Error>,
{
    async fn target(&self) -> &Target {
        &self.target
    }

    async fn outbox_state_view(&self) -> &OutboxStateView<C> {
        self.guard.deref()
    }
}

pub struct ChannelStateEntry<'a, C>
where
    C: Sync + Send + Context + 'static,
    ViewError: From<C::Error>,
{
    channel_name: ChannelName,
    guard: ReadGuardedView<'a, ChannelStateView<C>>,
}

#[Object]
impl<'a, C> ChannelStateEntry<'a, C>
where
    C: Sync + Send + Context + 'static + Clone,
    ViewError: From<C::Error>,
{
    async fn channel_name(&self) -> &ChannelName {
        &self.channel_name
    }

    async fn channel_state_view(&self) -> &ChannelStateView<C> {
        self.guard.deref()
    }
}
