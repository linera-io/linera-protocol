// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{Block, Event, Medium, Origin, Target},
    inbox::{InboxError, InboxStateView},
    outbox::OutboxStateView,
    ChainError, ChainManager,
};
use linera_base::{
    crypto::HashValue,
    data_types::{BlockHeight, ChainId, EffectId},
    ensure,
};
use linera_execution::{
    system::SystemEffect, ApplicationDescription, ApplicationId, ApplicationRegistryView,
    ChannelName, Destination, Effect, EffectContext, ExecutionResult, ExecutionRuntimeContext,
    ExecutionStateView, OperationContext, Query, QueryContext, RawExecutionResult, Response,
};
use linera_views::{
    collection_view::CollectionView,
    common::Context,
    log_view::LogView,
    register_view::RegisterView,
    set_view::SetView,
    views::{HashableContainerView, View, ViewError},
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// A view accessing the state of a chain.
#[derive(Debug, HashableContainerView)]
pub struct ChainStateView<C> {
    /// Execution state, including system and user applications.
    pub execution_state: ExecutionStateView<C>,
    /// Hash of the execution state.
    pub execution_state_hash: RegisterView<C, Option<HashValue>>,

    /// Block-chaining state.
    pub tip_state: RegisterView<C, ChainTipState>,

    /// Consensus state.
    pub manager: RegisterView<C, ChainManager>,

    /// Hashes of all certified blocks for this sender.
    /// This ends with `block_hash` and has length `usize::from(next_block_height)`.
    pub confirmed_log: LogView<C, HashValue>,
    /// Hashes of all certified blocks known as a receiver (local ordering).
    pub received_log: LogView<C, HashValue>,

    /// Communication state of applications.
    pub communication_states: CollectionView<C, ApplicationId, CommunicationStateView<C>>,

    /// The application bytecodes that have been published.
    pub application_registry: ApplicationRegistryView<C>,
}

/// Block-chaining state.
#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChainTipState {
    /// Hash of the latest certified block in this chain, if any.
    pub block_hash: Option<HashValue>,
    /// Sequence number tracking blocks.
    pub next_block_height: BlockHeight,
}

/// A view accessing the communication state of an application.
#[derive(Debug, HashableContainerView)]
pub struct CommunicationStateView<C> {
    /// Mailboxes used to receive messages indexed by their origin.
    pub inboxes: CollectionView<C, Origin, InboxStateView<C>>,
    /// Mailboxes used to send messages, indexed by their target.
    pub outboxes: CollectionView<C, Target, OutboxStateView<C>>,
    /// Channels able to multicast messages to subscribers.
    pub channels: CollectionView<C, ChannelName, ChannelStateView<C>>,
}

/// The state of a channel followed by subscribers.
#[derive(Debug, HashableContainerView)]
pub struct ChannelStateView<C> {
    /// The current subscribers.
    pub subscribers: SetView<C, ChainId>,
    /// The latest block height, if any, to be sent to future subscribers.
    pub block_height: RegisterView<C, Option<BlockHeight>>,
}

impl<C> ChainStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    pub fn chain_id(&self) -> ChainId {
        self.context().extra().chain_id()
    }

    pub async fn query_application(
        &mut self,
        application_id: ApplicationId,
        query: &Query,
    ) -> Result<Response, ChainError> {
        let context = QueryContext {
            chain_id: self.chain_id(),
        };
        let application = self.describe_application(application_id).await?;
        let response = self
            .execution_state
            .query_application(
                &application,
                &context,
                query,
                &mut self.application_registry,
            )
            .await?;
        Ok(response)
    }

    pub async fn mark_messages_as_received(
        &mut self,
        application_id: ApplicationId,
        target: Target,
        height: BlockHeight,
    ) -> Result<bool, ChainError> {
        let communication_state = self.communication_states.load_entry(application_id).await?;
        let outbox = communication_state
            .outboxes
            .load_entry(target.clone())
            .await?;
        let updated = outbox.mark_messages_as_received(height).await?;
        if updated && outbox.queue.count() == 0 {
            communication_state.outboxes.remove_entry(target)?;
        }
        Ok(updated)
    }

    /// Invariant for the states of active chains.
    pub fn is_active(&self) -> bool {
        self.execution_state.system.is_active()
    }

    /// Invariant for the states of active chains.
    pub fn ensure_is_active(&self) -> Result<(), ChainError> {
        if self.is_active() {
            Ok(())
        } else {
            Err(ChainError::InactiveChain(self.chain_id()))
        }
    }

    /// Verify that this chain is up-to-date and all the messages executed ahead of time
    /// have been properly received by now.
    pub async fn validate_incoming_messages(&mut self) -> Result<(), ChainError> {
        for id in self.communication_states.indices().await? {
            let state = self.communication_states.load_entry(id).await?;
            for origin in state.inboxes.indices().await? {
                let inbox = state.inboxes.load_entry(origin.clone()).await?;
                let event = inbox.removed_events.front().await?;
                ensure!(
                    event.is_none(),
                    ChainError::MissingCrossChainUpdate {
                        chain_id: self.chain_id(),
                        application_id: id,
                        origin,
                        height: event.unwrap().height,
                    }
                );
            }
        }
        Ok(())
    }

    pub async fn next_block_height_to_receive(
        &mut self,
        application_id: ApplicationId,
        origin: Origin,
    ) -> Result<BlockHeight, ChainError> {
        let communication_state = self.communication_states.load_entry(application_id).await?;
        let inbox = communication_state.inboxes.load_entry(origin).await?;
        inbox.next_block_height_to_receive()
    }

    pub async fn last_anticipated_block_height(
        &mut self,
        application_id: ApplicationId,
        origin: Origin,
    ) -> Result<Option<BlockHeight>, ChainError> {
        let communication_state = self.communication_states.load_entry(application_id).await?;
        let inbox = communication_state.inboxes.load_entry(origin).await?;
        match inbox.removed_events.back().await? {
            Some(event) => Ok(Some(event.height)),
            None => Ok(None),
        }
    }

    /// Schedule operations to be executed as a recipient, unless this block was already
    /// processed. Returns true if the call changed the chain state. Operations must be
    /// received by order of heights and indices.
    pub async fn receive_block(
        &mut self,
        application_id: ApplicationId,
        origin: &Origin,
        height: BlockHeight,
        effects: Vec<(ApplicationId, Destination, Effect)>,
        certificate_hash: HashValue,
    ) -> Result<(), ChainError> {
        let chain_id = self.chain_id();
        ensure!(
            height
                >= self
                    .next_block_height_to_receive(application_id, origin.clone())
                    .await?,
            ChainError::InternalError("Trying to receive blocks in the wrong order".to_string())
        );
        log::trace!(
            "Processing new messages to {:?} from {:?}::{:?} at height {}",
            chain_id,
            application_id,
            origin,
            height
        );
        // Process immediate effets and create inbox events.
        let mut events = Vec::new();
        for (index, (app_id, destination, effect)) in effects.into_iter().enumerate() {
            // Skip events that do not belong to this application.
            if app_id != application_id {
                continue;
            }
            // Skip events that do not belong to this origin OR have no effect on this
            // recipient.
            match destination {
                Destination::Recipient(id) => {
                    if origin.medium != Medium::Direct || id != chain_id {
                        continue;
                    }
                }
                Destination::Subscribers(name) => {
                    if !matches!(&origin.medium, Medium::Channel(n) if n == &name) {
                        continue;
                    }
                }
            }
            if let ApplicationId::System = app_id {
                // Handle special effects to be executed immediately.
                let effect_id = EffectId {
                    chain_id: origin.sender,
                    height,
                    index,
                };
                self.execute_immediate_effect(effect_id, &effect, chain_id)
                    .await?;
            }
            // Record the inbox event to process it below.
            events.push(Event {
                certificate_hash,
                height,
                index,
                effect,
            });
        }
        // There should be inbox events. Otherwise, this means the cross-chain request was
        // not routed correctly.
        ensure!(
            !events.is_empty(),
            ChainError::InternalError(format!(
                "The block received by {:?} from {:?} at height {:?} was entirely ignored. This should not happen",
                chain_id, origin, height))
        );
        // Process the inbox events and update the inbox state.
        let communication_state = self.communication_states.load_entry(application_id).await?;
        let inbox = communication_state
            .inboxes
            .load_entry(origin.clone())
            .await?;
        for event in events {
            inbox.add_event(event).await.map_err(|error| match error {
                InboxError::ViewError(error) => ChainError::ViewError(error),
                error => ChainError::InternalError(format!(
                    "while processing effects in certified block: {error}"
                )),
            })?;
        }
        // Remember the certificate for future validator/client synchronizations.
        self.received_log.push(certificate_hash);
        Ok(())
    }

    async fn execute_immediate_effect(
        &mut self,
        effect_id: EffectId,
        effect: &Effect,
        chain_id: ChainId,
    ) -> Result<(), ChainError> {
        match &effect {
            Effect::System(SystemEffect::OpenChain {
                id,
                owner,
                epoch,
                committees,
                admin_id,
            }) if id == &chain_id => {
                // Initialize ourself.
                self.execution_state.system.open_chain(
                    effect_id,
                    *id,
                    *owner,
                    *epoch,
                    committees.clone(),
                    *admin_id,
                );
                // Recompute the state hash.
                let hash = self.execution_state.hash_value().await?;
                self.execution_state_hash.set(Some(hash));
                // Last, reset the consensus state based on the current ownership.
                self.manager
                    .get_mut()
                    .reset(self.execution_state.system.ownership.get());
            }
            _ => {}
        }

        Ok(())
    }

    /// Execute a new block: first the incoming messages, then the main operation.
    /// * Modifies the state of inboxes, outboxes, and channels, if needed.
    /// * As usual, in case of errors, `self` may not be consistent any more and should be thrown away.
    /// * Returns the list of effects caused by the block being executed.
    pub async fn execute_block(
        &mut self,
        block: &Block,
    ) -> Result<Vec<(ApplicationId, Destination, Effect)>, ChainError> {
        assert_eq!(block.chain_id, self.chain_id());
        let chain_id = self.chain_id();
        let mut effects = Vec::new();
        for message in &block.incoming_messages {
            log::trace!(
                "Updating inbox {:?}::{:?} in chain {:?}",
                message.application_id,
                message.origin,
                chain_id
            );
            // Mark the message as processed in the inbox.
            let communication_state = self
                .communication_states
                .load_entry(message.application_id)
                .await?;
            let inbox = communication_state
                .inboxes
                .load_entry(message.origin.clone())
                .await?;
            inbox.remove_event(&message.event).await.map_err(|error| {
                ChainError::from((
                    chain_id,
                    message.application_id,
                    message.origin.clone(),
                    error,
                ))
            })?;
            // Execute the received effect.
            let context = EffectContext {
                chain_id,
                height: block.height,
                certificate_hash: message.event.certificate_hash,
                effect_id: EffectId {
                    chain_id: message.origin.sender,
                    height: message.event.height,
                    index: message.event.index,
                },
            };
            let application = self.describe_application(message.application_id).await?;
            let results = self
                .execution_state
                .execute_effect(
                    &application,
                    &context,
                    &message.event.effect,
                    &mut self.application_registry,
                )
                .await?;
            let communication_state = self
                .communication_states
                .load_entry(message.application_id)
                .await?;
            Self::process_execution_results(
                &mut communication_state.outboxes,
                &mut communication_state.channels,
                &mut effects,
                context.height,
                results,
            )
            .await?;
        }
        // Second, execute the operations in the block and remember the recipients to notify.
        for (index, (application_id, operation)) in block.operations.iter().enumerate() {
            let application = self.describe_application(*application_id).await?;
            let context = OperationContext {
                chain_id,
                height: block.height,
                index,
            };
            let communication_state = self
                .communication_states
                .load_entry(*application_id)
                .await?;
            let results = self
                .execution_state
                .execute_operation(
                    &application,
                    &context,
                    operation,
                    &mut self.application_registry,
                )
                .await?;

            Self::process_execution_results(
                &mut communication_state.outboxes,
                &mut communication_state.channels,
                &mut effects,
                context.height,
                results,
            )
            .await?;
        }
        // Recompute the state hash.
        let hash = self.execution_state.hash_value().await?;
        self.execution_state_hash.set(Some(hash));
        // Last, reset the consensus state based on the current ownership.
        self.manager
            .get_mut()
            .reset(self.execution_state.system.ownership.get());
        Ok(effects)
    }

    /// Register a new application in the chain state.
    ///
    /// Allows executing operations and effects for that application later.
    pub fn register_application(
        &mut self,
        application: ApplicationDescription,
    ) -> Result<ApplicationId, ChainError> {
        match application {
            ApplicationDescription::System => Ok(ApplicationId::System),
            ApplicationDescription::User(application) => Ok(ApplicationId::User(
                self.application_registry
                    .register_existing_application(application)?,
            )),
        }
    }

    /// Retrieve an application description.
    ///
    /// Retrieves the application description (with its bytecode location) from the internal map of
    /// applications known by this chain.
    pub async fn describe_application(
        &mut self,
        application_id: ApplicationId,
    ) -> Result<ApplicationDescription, ChainError> {
        match application_id {
            ApplicationId::System => Ok(ApplicationDescription::System),
            ApplicationId::User(id) => {
                let description = self.application_registry.describe_application(id).await?;
                Ok(ApplicationDescription::User(description))
            }
        }
    }

    async fn process_execution_results(
        outboxes: &mut CollectionView<C, Target, OutboxStateView<C>>,
        channels: &mut CollectionView<C, ChannelName, ChannelStateView<C>>,
        effects: &mut Vec<(ApplicationId, Destination, Effect)>,
        height: BlockHeight,
        results: Vec<ExecutionResult>,
    ) -> Result<(), ChainError> {
        for result in results {
            match result {
                ExecutionResult::System { result, .. } => {
                    Self::process_raw_execution_result(
                        ApplicationId::System,
                        outboxes,
                        channels,
                        effects,
                        height,
                        result,
                    )
                    .await?;
                }
                ExecutionResult::User(application_id, raw) => {
                    Self::process_raw_execution_result(
                        ApplicationId::User(application_id),
                        outboxes,
                        channels,
                        effects,
                        height,
                        raw,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn process_raw_execution_result<E: Into<Effect>>(
        application_id: ApplicationId,
        outboxes: &mut CollectionView<C, Target, OutboxStateView<C>>,
        channels: &mut CollectionView<C, ChannelName, ChannelStateView<C>>,
        effects: &mut Vec<(ApplicationId, Destination, Effect)>,
        height: BlockHeight,
        application: RawExecutionResult<E>,
    ) -> Result<(), ChainError> {
        // Record the effects of the execution. Effects are understood within an
        // application.
        let mut recipients = HashSet::new();
        let mut channel_broadcasts = HashSet::new();
        for (destination, effect) in application.effects {
            match &destination {
                Destination::Recipient(id) => {
                    recipients.insert(*id);
                }
                Destination::Subscribers(name) => {
                    channel_broadcasts.insert(name.clone());
                }
            }
            effects.push((application_id, destination, effect.into()));
        }

        // Update the (regular) outboxes.
        for recipient in recipients {
            let outbox = outboxes.load_entry(Target::chain(recipient)).await?;
            outbox.schedule_message(height)?;
        }

        // Update the channels.
        for (name, id) in application.unsubscribe {
            let channel = channels.load_entry(name).await?;
            // Remove subscriber. Do not remove the channel outbox yet.
            channel.subscribers.remove(&id)?;
        }
        for name in channel_broadcasts {
            let channel = channels.load_entry(name.clone()).await?;
            for recipient in channel.subscribers.indices().await? {
                let outbox = outboxes
                    .load_entry(Target::channel(recipient, name.clone()))
                    .await?;
                outbox.schedule_message(height)?;
            }
            channel.block_height.set(Some(height));
        }
        for (name, id) in application.subscribe {
            let channel = channels.load_entry(name.clone()).await?;
            // Add subscriber.
            if channel.subscribers.get(&id).await?.is_none() {
                // Send the latest message if any.
                if let Some(latest_height) = channel.block_height.get() {
                    let outbox = outboxes.load_entry(Target::channel(id, name)).await?;
                    outbox.schedule_message(*latest_height)?;
                }
            }
            channel.subscribers.insert(&id)?;
        }
        Ok(())
    }
}
