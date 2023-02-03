use crate::{
    chain::{ChainTipState, ChannelStateView, CommunicationStateView},
    data_types::{Event, Medium, Origin, Target},
    inbox::{Cursor, InboxStateView},
    outbox::OutboxStateView,
    ChainManager, ChainStateView,
};
use async_graphql::{scalar, Error, Object};
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, ChainId},
};
use linera_execution::{ApplicationId, ChannelName, ExecutionStateView};
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

#[Object]
impl<C: Context + Send + Sync + Clone + 'static> ChainStateView<C>
where
    ViewError: From<C::Error>,
{
    async fn execution_state_view(&self) -> &ExecutionStateView<C> {
        &self.execution_state
    }

    async fn tip_state(&self) -> &ChainTipState {
        self.tip_state.get()
    }

    async fn execution_state_hash(&self) -> &Option<CryptoHash> {
        self.execution_state_hash.get()
    }

    async fn manager(&self) -> &ChainManager {
        self.manager.get()
    }

    async fn confirmed_log(&self, range: Option<Range>) -> Result<Vec<CryptoHash>, Error> {
        let range = range.unwrap_or(Range {
            start: 0,
            end: self.confirmed_log.count(),
        });
        Ok(self.confirmed_log.read(range.into()).await?)
    }

    async fn received_log(&self, range: Option<Range>) -> Result<Vec<CryptoHash>, Error> {
        let range = range.unwrap_or(Range {
            start: 0,
            end: self.received_log.count(),
        });
        Ok(self.received_log.read(range.into()).await?)
    }

    async fn communication_states(
        &self,
        application_id: ApplicationId,
    ) -> Result<CommunicationStateElement<C>, Error> {
        Ok(CommunicationStateElement {
            application_id,
            guard: self
                .communication_states
                .try_load_entry(application_id)
                .await?,
        })
    }

    async fn communication_state_indices(&self) -> Result<Vec<ApplicationId>, Error> {
        Ok(self.communication_states.indices().await?)
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
    async fn inboxes(
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

    async fn inboxes_indices(&self) -> Result<Vec<Origin>, Error> {
        Ok(self.inboxes.indices().await?)
    }

    async fn outboxes(
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

    async fn outboxes_indices(&self) -> Result<Vec<Target>, Error> {
        Ok(self.outboxes.indices().await?)
    }

    async fn channels(&self, channel_name: ChannelName) -> Result<ChannelStateElement<C>, Error> {
        Ok(ChannelStateElement {
            channel_name: channel_name.clone(),
            guard: self.channels.try_load_entry(channel_name).await?,
        })
    }

    async fn channels_indices(&self) -> Result<Vec<ChannelName>, Error> {
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

#[Object]
impl<C: Sync + Send + Context + Clone + 'static> InboxStateView<C>
where
    ViewError: From<C::Error>,
{
    async fn next_cursor_to_add(&self) -> &Cursor {
        self.next_cursor_to_add.get()
    }

    async fn next_cursor_to_remove(&self) -> &Cursor {
        self.next_cursor_to_remove.get()
    }

    async fn added_events(&self, count: Option<usize>) -> Result<Vec<Event>, Error> {
        let count = count.unwrap_or_else(|| self.added_events.count());
        Ok(self.added_events.read_front(count).await?)
    }

    async fn removed_events(&self, count: Option<usize>) -> Result<Vec<Event>, Error> {
        let count = count.unwrap_or_else(|| self.removed_events.count());
        Ok(self.removed_events.read_front(count).await?)
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

#[Object]
impl<C: Sync + Send + Context + Clone + 'static> OutboxStateView<C>
where
    ViewError: From<C::Error>,
{
    async fn next_height_to_schedule(&self) -> &BlockHeight {
        self.next_height_to_schedule.get()
    }

    async fn queue(&self, count: Option<usize>) -> Result<Vec<BlockHeight>, Error> {
        let count = count.unwrap_or_else(|| self.queue.count());
        Ok(self.queue.read_front(count).await?)
    }
}

struct ChannelStateElement<'a, C>
where
    C: Sync + Send + Context + 'static,
    ViewError: From<C::Error>,
{
    channel_name: ChannelName,
    guard: ReadGuardedView<'a, ChannelStateView<C>>,
}

#[Object]
impl<'a, C> ChannelStateElement<'a, C>
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

#[Object]
impl<C: Sync + Send + Context + Clone + 'static> ChannelStateView<C>
where
    ViewError: From<C::Error>,
{
    async fn subscribers(&self) -> Result<Vec<ChainId>, Error> {
        Ok(self.subscribers.indices().await?)
    }

    async fn block_height(&self) -> &Option<BlockHeight> {
        self.block_height.get()
    }
}
