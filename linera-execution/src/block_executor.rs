// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, vec};

use custom_debug_derive::Debug;
use linera_base::{
    crypto::BcsHashable,
    data_types::{Amount, Blob, Event, OracleResponse, Timestamp},
    doc_scalar, hex_debug,
    identifiers::{AccountOwner, BlobType, ChainId, ChannelFullName, Destination, Owner},
};
use linera_views::{context::Context, views::View};
use serde::{Deserialize, Serialize};

use crate::{
    resources::Sources, ExecutionError, ExecutionRuntimeContext, ExecutionStateView, Message,
    MessageContext, MessageKind, Operation, OperationContext, OutgoingMessage, ResourceController,
    ResourceTracker, SystemMessage, TransactionTracker,
};

/// The execution result of a single operation.
#[derive(Debug, Default, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct OperationResult(
    #[debug(with = "hex_debug")]
    #[serde(with = "serde_bytes")]
    pub Vec<u8>,
);

impl<'de> BcsHashable<'de> for OperationResult {}

doc_scalar!(
    OperationResult,
    "The execution result of a single operation."
);

pub struct BlockExecutor<'a, C> {
    pub state: &'a mut ExecutionStateView<C>,
    pub resource_controller: ResourceController<Option<Owner>>,
    replaying_oracle_responses: Option<vec::IntoIter<Vec<OracleResponse>>>,
    next_message_index: u32,
    next_application_index: u32,
    local_time: Timestamp,
    pub oracle_responses: Vec<Vec<OracleResponse>>,
    pub events: Vec<Vec<Event>>,
    pub blobs: Vec<Vec<Blob>>,
    pub messages: Vec<Vec<OutgoingMessage>>,
    pub operation_results: Vec<OperationResult>,
    pub subscribe: Vec<(ChannelFullName, ChainId)>,
    pub unsubscribe: Vec<(ChannelFullName, ChainId)>,
}

impl<'a, C> BlockExecutor<'a, C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    pub fn new(
        state: &'a mut ExecutionStateView<C>,
        account: Option<Owner>,
        replaying_oracle_responses: Option<Vec<Vec<OracleResponse>>>,
        local_time: Timestamp,
    ) -> Result<Self, ExecutionError> {
        let (_, committee) = state
            .system
            .current_committee()
            .ok_or_else(|| ExecutionError::InactiveChain)?;
        let resource_controller = ResourceController {
            policy: Arc::new(committee.policy().clone()),
            tracker: ResourceTracker::default(),
            account,
        };
        Ok(Self {
            state,
            resource_controller,
            replaying_oracle_responses: replaying_oracle_responses.map(Vec::into_iter),
            next_message_index: 0,
            next_application_index: 0,
            local_time,
            oracle_responses: Vec::new(),
            events: Vec::new(),
            blobs: Vec::new(),
            messages: Vec::new(),
            operation_results: Vec::new(),
            subscribe: Vec::new(),
            unsubscribe: Vec::new(),
        })
    }

    pub async fn track_block(
        &mut self,
        empty_block_size: usize,
        incoming_bundle_count: usize,
        operation_count: usize,
        published_blobs: &[Blob],
    ) -> Result<(), ExecutionError> {
        self.resource_controller
            .track_block_size(empty_block_size)?;
        self.resource_controller
            .track_executed_block_size_sequence_extension(0, incoming_bundle_count, 1)?;
        self.resource_controller
            .track_executed_block_size_sequence_extension(0, operation_count, 1)?;
        for blob in published_blobs {
            let blob_type = blob.content().blob_type();
            if blob_type == BlobType::Data
                || blob_type == BlobType::ContractBytecode
                || blob_type == BlobType::ServiceBytecode
            {
                self.resource_controller_with_state()
                    .await?
                    .track_blob_published(blob.content())?;
            }
            self.state.system.used_blobs.insert(&blob.id())?;
        }
        Ok(())
    }

    pub fn new_transaction<'b>(
        &'b mut self,
    ) -> Result<TransactionExecutor<'a, 'b, C>, ExecutionError> {
        let maybe_responses = match self.replaying_oracle_responses.as_mut().map(Iterator::next) {
            Some(Some(responses)) => Some(responses),
            Some(None) => return Err(ExecutionError::MissingOracleResponseList),
            None => None,
        };
        let transaction_tracker = TransactionTracker::new(
            self.next_message_index,
            self.next_application_index,
            maybe_responses,
        );
        Ok(TransactionExecutor {
            block_executor: self,
            transaction_tracker,
        })
    }

    pub async fn resource_controller_with_state(
        &mut self,
    ) -> Result<ResourceController<Sources, &mut ResourceTracker>, ExecutionError> {
        Ok(self
            .resource_controller
            .with_state(&mut self.state.system)
            .await?)
    }

    fn chain_id(&self) -> ChainId {
        self.state.context().extra().chain_id()
    }
}

pub struct TransactionExecutor<'a, 'b, C> {
    pub block_executor: &'b mut BlockExecutor<'a, C>,
    pub transaction_tracker: TransactionTracker,
}

impl<'a, 'b, C> TransactionExecutor<'a, 'b, C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    /// Executes an operation.
    pub async fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: Operation,
    ) -> Result<(), ExecutionError> {
        self.block_executor
            .resource_controller_with_state()
            .await?
            .track_operation(&operation)?;
        self.block_executor
            .state
            .execute_operation(
                context,
                self.block_executor.local_time,
                operation,
                &mut self.transaction_tracker,
                &mut self.block_executor.resource_controller,
            )
            .await
    }

    /// Executes a message as part of an incoming bundle in a block.
    /// Refunds the remainder of the grant, if any.
    pub async fn execute_message(
        &mut self,
        context: MessageContext,
        message: Message,
        mut grant: Amount,
    ) -> Result<(), ExecutionError> {
        self.block_executor
            .state
            .execute_message(
                context,
                self.block_executor.local_time,
                message,
                (grant > Amount::ZERO).then_some(&mut grant),
                &mut self.transaction_tracker,
                &mut self.block_executor.resource_controller,
            )
            .await?;
        if grant > Amount::ZERO {
            self.send_refund(context, grant).await?;
        }
        Ok(())
    }

    pub async fn bounce_message(
        &mut self,
        context: MessageContext,
        grant: Amount,
        message: Message,
    ) -> Result<(), ExecutionError> {
        assert_eq!(context.chain_id, self.block_executor.chain_id());
        self.transaction_tracker
            .add_outgoing_message(OutgoingMessage {
                destination: Destination::Recipient(context.message_id.chain_id),
                authenticated_signer: context.authenticated_signer,
                refund_grant_to: context.refund_grant_to.filter(|_| !grant.is_zero()),
                grant,
                kind: MessageKind::Bouncing,
                message,
            })?;
        Ok(())
    }

    pub async fn send_refund(
        &mut self,
        context: MessageContext,
        amount: Amount,
    ) -> Result<(), ExecutionError> {
        assert_eq!(context.chain_id, self.block_executor.chain_id());
        // Nothing to do except maybe refund the grant.
        let Some(refund_grant_to) = context.refund_grant_to else {
            // See OperationContext::refund_grant_to()
            return Err(ExecutionError::InternalError(
                "Messages with grants should have a non-empty `refund_grant_to`",
            ));
        };
        let message = SystemMessage::Credit {
            amount,
            source: context
                .authenticated_signer
                .map(AccountOwner::User)
                .unwrap_or(AccountOwner::Chain),
            target: refund_grant_to.owner,
        };
        self.transaction_tracker.add_outgoing_message(
            OutgoingMessage::new(refund_grant_to.chain_id, message).with_kind(MessageKind::Tracked),
        )?;
        Ok(())
    }

    pub fn track_block_size_of(&mut self, data: &impl Serialize) -> Result<(), ExecutionError> {
        self.block_executor
            .resource_controller
            .track_block_size_of(data)
    }

    pub async fn finalize_transaction(
        self,
        is_accepted_message: bool,
        is_operation: bool,
    ) -> Result<(), ExecutionError> {
        let txn_outcome = self.transaction_tracker.into_outcome()?;

        self.block_executor.next_message_index = txn_outcome.next_message_index;
        self.block_executor.next_application_index = txn_outcome.next_application_index;

        let transaction_count = self.block_executor.oracle_responses.len();
        let operation_count = self.block_executor.operation_results.len();

        let mut resource_controller = self.block_executor.resource_controller_with_state().await?;
        if is_accepted_message || is_operation {
            for message_out in &txn_outcome.outgoing_messages {
                resource_controller.track_message(&message_out.message)?;
            }
        }
        resource_controller.track_block_size_of(&(
            &txn_outcome.oracle_responses,
            &txn_outcome.outgoing_messages,
            &txn_outcome.events,
            &txn_outcome.blobs,
        ))?;
        for blob in &txn_outcome.blobs {
            if blob.content().blob_type() == BlobType::Data {
                resource_controller.track_blob_published(blob.content())?;
            }
        }
        // The oracles, messages, blobs and events collections are each extended by one, and all
        // have the same length.
        resource_controller.track_executed_block_size_sequence_extension(
            transaction_count,
            1,
            4,
        )?;

        if is_operation {
            resource_controller.track_block_size_of(&(&txn_outcome.operation_result))?;
            resource_controller.track_executed_block_size_sequence_extension(
                operation_count,
                1,
                1,
            )?;
        }

        self.block_executor
            .oracle_responses
            .push(txn_outcome.oracle_responses);
        self.block_executor
            .messages
            .push(txn_outcome.outgoing_messages);
        self.block_executor.subscribe.extend(txn_outcome.subscribe);
        self.block_executor
            .unsubscribe
            .extend(txn_outcome.unsubscribe);
        self.block_executor.events.push(txn_outcome.events);

        self.block_executor.blobs.push(txn_outcome.blobs);

        if is_operation {
            self.block_executor
                .operation_results
                .push(OperationResult(txn_outcome.operation_result));
        }

        Ok(())
    }
}
