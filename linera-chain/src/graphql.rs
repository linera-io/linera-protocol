use crate::{
    chain::{ChainTipState, ChannelStateView, CommunicationStateView},
    data_types::{Event, Medium, Origin, Target},
    inbox::{Cursor, InboxStateView},
    outbox::OutboxStateView,
    ChainManager, ChainStateView,
};
use async_graphql::{scalar, Error, Object, OutputType};
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, ChainId},
};
use linera_execution::{ApplicationId, ChannelName, ExecutionStateView};
use linera_views::{
    collection_view::ReadGuardedView,
    common::Context,
    views::{View, ViewError},
};
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, ops::Deref};

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

// TODO move to `linera_views`.
struct CollectionElement<'a, C: Sync + Send + Context + 'static, I, W>
where
    ViewError: From<C::Error>,
    W: View<C> + Sync,
    I: Send + Sync,
{
    index: I,
    guard: ReadGuardedView<'a, W>,
    _phantom: PhantomData<C>,
}

// TODO move to `linera_views`.
#[Object]
impl<'a, C: Sync + Send + Context + 'static, I, W> CollectionElement<'a, C, I, W>
where
    ViewError: From<C::Error>,
    W: View<C> + Sync + OutputType + 'a,
    I: Send + Sync + OutputType,
{
    async fn index(&self) -> &I {
        &self.index
    }

    async fn view(&self) -> &W {
        self.guard.deref()
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
    ) -> Result<Vec<CollectionElement<C, ApplicationId, CommunicationStateView<C>>>, Error> {
        let mut communication_states = vec![];
        for application_id in self.communication_states.indices().await? {
            communication_states.push(CollectionElement {
                index: application_id,
                guard: self
                    .communication_states
                    .try_load_entry(application_id)
                    .await?,
                _phantom: Default::default(),
            });
        }
        Ok(communication_states)
    }
}

#[Object]
impl<C: Sync + Send + Context + Clone + 'static> CommunicationStateView<C>
where
    ViewError: From<C::Error>,
{
    async fn inboxes(&self) -> Result<Vec<CollectionElement<C, Origin, InboxStateView<C>>>, Error> {
        let mut inbox_states = vec![];
        for origin in self.inboxes.indices().await? {
            inbox_states.push(CollectionElement {
                index: origin.clone(),
                guard: self.inboxes.try_load_entry(origin).await?,
                _phantom: Default::default(),
            });
        }
        Ok(inbox_states)
    }

    async fn outboxes(
        &self,
    ) -> Result<Vec<CollectionElement<C, Target, OutboxStateView<C>>>, Error> {
        let mut outbox_states = vec![];
        for target in self.outboxes.indices().await? {
            outbox_states.push(CollectionElement {
                index: target.clone(),
                guard: self.outboxes.try_load_entry(target).await?,
                _phantom: Default::default(),
            });
        }
        Ok(outbox_states)
    }

    async fn channels(
        &self,
    ) -> Result<Vec<CollectionElement<C, ChannelName, ChannelStateView<C>>>, Error> {
        let mut channel_states = vec![];
        for channel_name in self.channels.indices().await? {
            channel_states.push(CollectionElement {
                index: channel_name.clone(),
                guard: self.channels.try_load_entry(channel_name).await?,
                _phantom: Default::default(),
            });
        }
        Ok(channel_states)
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
