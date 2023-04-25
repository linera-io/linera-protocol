// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{
        Block, ChainAndHeight, ChannelFullName, Event, Medium, Origin, OutgoingEffect, Target,
    },
    inbox::{InboxError, InboxStateView},
    outbox::OutboxStateView,
    ChainError, ChainManager,
};
use async_graphql::SimpleObject;
use linera_base::{
    crypto::CryptoHash,
    data_types::{ArithmeticError, BlockHeight, Timestamp},
    ensure,
    identifiers::{ChainId, Destination, EffectId},
};
use linera_execution::{
    system::SystemEffect, ApplicationId, Effect, EffectContext, ExecutionResult,
    ExecutionRuntimeContext, ExecutionStateView, OperationContext, Query, QueryContext,
    RawExecutionResult, Response, UserApplicationDescription, UserApplicationId,
};
use linera_views::{
    collection_view::CollectionView,
    common::Context,
    log_view::LogView,
    register_view::RegisterView,
    set_view::SetView,
    views::{CryptoHashView, GraphQLView, RootView, View, ViewError},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

/// A view accessing the state of a chain.
#[derive(Debug, RootView, GraphQLView)]
pub struct ChainStateView<C> {
    /// Execution state, including system and user applications.
    pub execution_state: ExecutionStateView<C>,
    /// Hash of the execution state.
    pub execution_state_hash: RegisterView<C, Option<CryptoHash>>,

    /// Block-chaining state.
    pub tip_state: RegisterView<C, ChainTipState>,

    /// Consensus state.
    pub manager: RegisterView<C, ChainManager>,

    /// Hashes of all certified blocks for this sender.
    /// This ends with `block_hash` and has length `usize::from(next_block_height)`.
    pub confirmed_log: LogView<C, CryptoHash>,
    /// Sender chain and height of all certified blocks known as a receiver (local ordering).
    pub received_log: LogView<C, ChainAndHeight>,

    /// Mailboxes used to receive messages indexed by their origin.
    pub inboxes: CollectionView<C, Origin, InboxStateView<C>>,
    /// Mailboxes used to send messages, indexed by their target.
    pub outboxes: CollectionView<C, Target, OutboxStateView<C>>,
    /// Number of outgoing messages in flight for each block height.
    /// We use a `RegisterView` to prioritize speed for small maps.
    pub outbox_counters: RegisterView<C, BTreeMap<BlockHeight, u32>>,
    /// Channels able to multicast messages to subscribers.
    pub channels: CollectionView<C, ChannelFullName, ChannelStateView<C>>,
}

/// Block-chaining state.
#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize, SimpleObject)]
pub struct ChainTipState {
    /// Hash of the latest certified block in this chain, if any.
    pub block_hash: Option<CryptoHash>,
    /// Sequence number tracking blocks.
    pub next_block_height: BlockHeight,
}

/// The state of a channel followed by subscribers.
#[derive(Debug, View, GraphQLView)]
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

    pub async fn query_application(&mut self, query: &Query) -> Result<Response, ChainError> {
        let context = QueryContext {
            chain_id: self.chain_id(),
        };
        let response = self
            .execution_state
            .query_application(&context, query)
            .await?;
        Ok(response)
    }

    pub async fn describe_application(
        &mut self,
        application_id: UserApplicationId,
    ) -> Result<UserApplicationDescription, ChainError> {
        self.execution_state
            .system
            .registry
            .describe_application(application_id)
            .await
            .map_err(|err| ChainError::ExecutionError(err.into()))
    }

    pub async fn mark_messages_as_received(
        &mut self,
        target: Target,
        height: BlockHeight,
    ) -> Result<bool, ChainError> {
        let outbox = self.outboxes.load_entry_mut(&target).await?;
        let updates = outbox.mark_messages_as_received(height).await?;
        if updates.is_empty() {
            return Ok(false);
        }
        for update in updates {
            let counter = self
                .outbox_counters
                .get_mut()
                .get_mut(&update)
                .expect("message counter should be present");
            *counter = counter
                .checked_sub(1)
                .expect("message counter should not underflow");
            if *counter == 0 {
                // Important for the test in `all_messages_delivered_up_to`.
                self.outbox_counters.get_mut().remove(&update);
            }
        }
        if outbox.queue.count() == 0 {
            self.outboxes.remove_entry(&target)?;
        }
        Ok(true)
    }

    /// Returns true if there is no more outgoing messages in flight up to the given
    /// block height.
    pub fn all_messages_delivered_up_to(&mut self, height: BlockHeight) -> bool {
        tracing::debug!(
            "Messages left in {:?}'s outbox: {:?}",
            self.chain_id(),
            self.outbox_counters.get()
        );
        if let Some((key, _)) = self.outbox_counters.get().first_key_value() {
            key > &height
        } else {
            true
        }
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
        let chain_id = self.chain_id();
        for origin in self.inboxes.indices().await? {
            let inbox = self.inboxes.load_entry(&origin).await?;
            let event = inbox.removed_events.front().await?;
            ensure!(
                event.is_none(),
                ChainError::MissingCrossChainUpdate {
                    chain_id,
                    origin,
                    height: event.unwrap().height,
                }
            );
        }
        Ok(())
    }

    pub async fn next_block_height_to_receive(
        &mut self,
        origin: Origin,
    ) -> Result<BlockHeight, ChainError> {
        let inbox = self.inboxes.load_entry(&origin).await?;
        inbox.next_block_height_to_receive()
    }

    pub async fn last_anticipated_block_height(
        &mut self,
        origin: Origin,
    ) -> Result<Option<BlockHeight>, ChainError> {
        let inbox = self.inboxes.load_entry(&origin).await?;
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
        origin: &Origin,
        height: BlockHeight,
        timestamp: Timestamp,
        effects: Vec<OutgoingEffect>,
        certificate_hash: CryptoHash,
    ) -> Result<(), ChainError> {
        let chain_id = self.chain_id();
        ensure!(
            height >= self.next_block_height_to_receive(origin.clone()).await?,
            ChainError::InternalError("Trying to receive blocks in the wrong order".to_string())
        );
        tracing::trace!(
            "Processing new messages to {:?} from {:?} at height {}",
            chain_id,
            origin,
            height
        );
        // Process immediate effects and create inbox events.
        let mut events = Vec::new();
        for (index, outgoing_effect) in effects.into_iter().enumerate() {
            let index = u32::try_from(index).map_err(|_| ArithmeticError::Overflow)?;
            let OutgoingEffect {
                destination,
                authenticated_signer,
                effect,
            } = outgoing_effect;
            // Skip events that do not belong to this origin OR have no effect on this
            // recipient.
            match destination {
                Destination::Recipient(id) => {
                    if origin.medium != Medium::Direct || id != chain_id {
                        continue;
                    }
                }
                Destination::Subscribers(name) => {
                    let expected_medium = Medium::Channel(ChannelFullName {
                        application_id: effect.application_id(),
                        name,
                    });
                    if origin.medium != expected_medium {
                        continue;
                    }
                }
            }
            if let Effect::System(_) = effect {
                // Handle special effects to be executed immediately.
                let effect_id = EffectId {
                    chain_id: origin.sender,
                    height,
                    index,
                };
                self.execute_immediate_effect(effect_id, &effect, timestamp)
                    .await?;
            }
            // Record the inbox event to process it below.
            events.push(Event {
                certificate_hash,
                height,
                index,
                authenticated_signer,
                timestamp,
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
        let inbox = self.inboxes.load_entry_mut(origin).await?;
        for event in events {
            inbox.add_event(event).await.map_err(|error| match error {
                InboxError::ViewError(error) => ChainError::ViewError(error),
                error => ChainError::InternalError(format!(
                    "while processing effects in certified block: {error}"
                )),
            })?;
        }
        // Remember the certificate for future validator/client synchronizations.
        self.received_log.push(ChainAndHeight {
            chain_id: origin.sender,
            height,
        });
        Ok(())
    }

    async fn execute_immediate_effect(
        &mut self,
        effect_id: EffectId,
        effect: &Effect,
        timestamp: Timestamp,
    ) -> Result<(), ChainError> {
        if let Effect::System(SystemEffect::OpenChain {
            public_key,
            epoch,
            committees,
            admin_id,
        }) = effect
        {
            // Initialize ourself.
            self.execution_state.system.open_chain(
                effect_id,
                *public_key,
                *epoch,
                committees.clone(),
                *admin_id,
                timestamp,
            );
            // Recompute the state hash.
            let hash = self.execution_state.crypto_hash().await?;
            self.execution_state_hash.set(Some(hash));
            // Last, reset the consensus state based on the current ownership.
            self.manager
                .get_mut()
                .reset(self.execution_state.system.ownership.get());
        }
        Ok(())
    }

    /// Removes the incoming messages in the block from the inboxes.
    pub async fn remove_events_from_inboxes(&mut self, block: &Block) -> Result<(), ChainError> {
        let chain_id = self.chain_id();
        for message in &block.incoming_messages {
            tracing::trace!(
                "Updating inbox {:?} in chain {:?}",
                message.origin,
                chain_id
            );
            if message.event.timestamp > block.timestamp {
                return Err(ChainError::IncorrectEventTimestamp {
                    chain_id,
                    message_timestamp: message.event.timestamp,
                    block_timestamp: block.timestamp,
                });
            }
            // Mark the message as processed in the inbox.
            let inbox = self.inboxes.load_entry_mut(&message.origin).await?;
            inbox
                .remove_event(&message.event)
                .await
                .map_err(|error| ChainError::from((chain_id, message.origin.clone(), error)))?;
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
    ) -> Result<Vec<OutgoingEffect>, ChainError> {
        assert_eq!(block.chain_id, self.chain_id());
        let chain_id = self.chain_id();
        ensure!(
            *self.execution_state.system.timestamp.get() <= block.timestamp,
            ChainError::InvalidBlockTimestamp
        );
        self.execution_state.system.timestamp.set(block.timestamp);
        self.execution_state.add_fuel(10_000_000);
        let mut effects = Vec::new();
        for message in &block.incoming_messages {
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
                authenticated_signer: message.event.authenticated_signer,
            };
            let results = self
                .execution_state
                .execute_effect(&context, &message.event.effect)
                .await?;
            self.process_execution_results(&mut effects, context.height, results)
                .await?;
        }
        // Second, execute the operations in the block and remember the recipients to notify.
        for (index, operation) in block.operations.iter().enumerate() {
            let index = u32::try_from(index).map_err(|_| ArithmeticError::Overflow)?;
            let context = OperationContext {
                chain_id,
                height: block.height,
                index,
                authenticated_signer: block.authenticated_signer,
            };
            let results = self
                .execution_state
                .execute_operation(&context, operation)
                .await?;
            self.process_execution_results(&mut effects, context.height, results)
                .await?;
        }
        // Recompute the state hash.
        let hash = self.execution_state.crypto_hash().await?;
        self.execution_state_hash.set(Some(hash));
        // Last, reset the consensus state based on the current ownership.
        self.manager
            .get_mut()
            .reset(self.execution_state.system.ownership.get());
        Ok(effects)
    }

    async fn process_execution_results(
        &mut self,
        effects: &mut Vec<OutgoingEffect>,
        height: BlockHeight,
        results: Vec<ExecutionResult>,
    ) -> Result<(), ChainError> {
        for result in results {
            match result {
                ExecutionResult::System(result) => {
                    Self::process_raw_execution_result(
                        ApplicationId::System,
                        Effect::System,
                        &mut self.outboxes,
                        self.outbox_counters.get_mut(),
                        &mut self.channels,
                        effects,
                        height,
                        result,
                    )
                    .await?;
                }
                ExecutionResult::User(application_id, result) => {
                    Self::process_raw_execution_result(
                        ApplicationId::User(application_id),
                        |bytes| Effect::User {
                            application_id,
                            bytes,
                        },
                        &mut self.outboxes,
                        self.outbox_counters.get_mut(),
                        &mut self.channels,
                        effects,
                        height,
                        result,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_raw_execution_result<E, F>(
        application_id: ApplicationId,
        lift: F,
        outboxes: &mut CollectionView<C, Target, OutboxStateView<C>>,
        outbox_counters: &mut BTreeMap<BlockHeight, u32>,
        channels: &mut CollectionView<C, ChannelFullName, ChannelStateView<C>>,
        effects: &mut Vec<OutgoingEffect>,
        height: BlockHeight,
        raw_result: RawExecutionResult<E>,
    ) -> Result<(), ChainError>
    where
        F: Fn(E) -> Effect,
    {
        // Record the effects of the execution. Effects are understood within an
        // application.
        let mut recipients = HashSet::new();
        let mut channel_broadcasts = HashSet::new();
        for (destination, authenticated, effect) in raw_result.effects {
            match &destination {
                Destination::Recipient(id) => {
                    recipients.insert(*id);
                }
                Destination::Subscribers(name) => {
                    channel_broadcasts.insert(name.clone());
                }
            }
            let authenticated_signer = if authenticated {
                raw_result.authenticated_signer
            } else {
                None
            };
            effects.push(OutgoingEffect {
                destination,
                authenticated_signer,
                effect: lift(effect),
            });
        }

        // Update the (regular) outboxes.
        for recipient in recipients {
            let outbox = outboxes.load_entry_mut(&Target::chain(recipient)).await?;
            if outbox.schedule_message(height)? {
                *outbox_counters.entry(height).or_default() += 1;
            }
        }

        // Update the channels.
        for (name, id) in raw_result.unsubscribe {
            let full_name = ChannelFullName {
                application_id,
                name,
            };
            let channel = channels.load_entry_mut(&full_name).await?;
            // Remove subscriber. Do not remove the channel outbox yet.
            channel.subscribers.remove(&id)?;
        }
        for name in channel_broadcasts {
            let full_name = ChannelFullName {
                application_id,
                name,
            };
            let channel = channels.load_entry_mut(&full_name).await?;
            for recipient in channel.subscribers.indices().await? {
                let outbox = outboxes
                    .load_entry_mut(&Target::channel(recipient, full_name.clone()))
                    .await?;
                if outbox.schedule_message(height)? {
                    *outbox_counters.entry(height).or_default() += 1;
                }
            }
            channel.block_height.set(Some(height));
        }
        for (name, id) in raw_result.subscribe {
            let full_name = ChannelFullName {
                application_id,
                name,
            };
            let channel = channels.load_entry_mut(&full_name).await?;
            // Add subscriber.
            if !channel.subscribers.contains(&id).await? {
                // Send the latest message if any.
                if let Some(latest_height) = channel.block_height.get() {
                    let outbox = outboxes
                        .load_entry_mut(&Target::channel(id, full_name.clone()))
                        .await?;
                    if outbox.schedule_message(*latest_height)? {
                        *outbox_counters.entry(*latest_height).or_default() += 1;
                    }
                }
                channel.subscribers.insert(&id)?;
            }
        }
        Ok(())
    }
}
