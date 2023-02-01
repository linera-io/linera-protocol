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
use std::{marker::PhantomData, ops::Deref};

scalar!(ChainManager);
scalar!(Event);
scalar!(Medium);

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

    async fn confirmed_log(&self) -> usize {
        self.confirmed_log.count()
    }

    async fn received_log(&self) -> usize {
        self.received_log.count()
    }

    async fn communication_states(
        &self,
    ) -> Vec<CollectionElement<C, ApplicationId, CommunicationStateView<C>>> {
        let mut communication_states = vec![];
        for application_id in self.communication_states.indices().await.unwrap() {
            communication_states.push(CollectionElement {
                index: application_id.clone(),
                guard: self
                    .communication_states
                    .load_entry(application_id)
                    .await
                    .unwrap(),
                _phantom: Default::default(),
            });
        }
        communication_states
    }
}

#[Object]
impl<C: Sync + Send + Context + Clone + 'static> CommunicationStateView<C>
where
    ViewError: From<C::Error>,
{
    async fn inboxes(&self) -> Vec<CollectionElement<C, Origin, InboxStateView<C>>> {
        let mut inbox_states = vec![];
        for origin in self.inboxes.indices().await.unwrap() {
            inbox_states.push(CollectionElement {
                index: origin.clone(),
                guard: self.inboxes.load_entry(origin).await.unwrap(),
                _phantom: Default::default(),
            });
        }
        inbox_states
    }

    async fn outboxes(&self) -> Vec<CollectionElement<C, Target, OutboxStateView<C>>> {
        let mut outbox_states = vec![];
        for target in self.outboxes.indices().await.unwrap() {
            outbox_states.push(CollectionElement {
                index: target.clone(),
                guard: self.outboxes.load_entry(target).await.unwrap(),
                _phantom: Default::default(),
            });
        }
        outbox_states
    }

    async fn channels(&self) -> Vec<CollectionElement<C, ChannelName, ChannelStateView<C>>> {
        let mut channel_states = vec![];
        for channel_name in self.channels.indices().await.unwrap() {
            channel_states.push(CollectionElement {
                index: channel_name.clone(),
                guard: self.channels.load_entry(channel_name).await.unwrap(),
                _phantom: Default::default(),
            });
        }
        channel_states
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
        let count = count.unwrap_or(self.added_events.count());
        Ok(self.added_events.read_front(count).await?)
    }

    async fn removed_events(&self, count: Option<usize>) -> Result<Vec<Event>, Error> {
        let count = count.unwrap_or(self.removed_events.count());
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
        let count = count.unwrap_or(self.queue.count());
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
