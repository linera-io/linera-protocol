// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A builder of [`Block`]s which are then signed to become [`Certificate`]s.
//!
//! Helps with the construction of blocks, adding operations and

use linera_base::{
    abi::ContractAbi,
    data_types::{Amount, ApplicationPermissions, Blob, Epoch, Resources, Round, Timestamp},
    identifiers::{AccountOwner, ApplicationId, ChainId, MessageId},
    ownership::TimeoutConfig,
};
use linera_chain::{
    data_types::{
        IncomingBundle, LiteValue, LiteVote, MessageAction, ProposedBlock, SignatureAggregator,
    },
    types::{BlockHeader, ConfirmedBlock, ConfirmedBlockCertificate},
};
use linera_core::worker::WorkerError;
use linera_execution::{
    system::{Recipient, SystemOperation},
    Operation, OutgoingMessage, ResourceTracker,
};

use super::TestValidator;

/// A helper type to build a block proposal using the builder pattern, and then signing them into
/// [`ConfirmedBlockCertificate`]s using a [`TestValidator`].
pub struct BlockBuilder {
    block: ProposedBlock,
    validator: TestValidator,
}

impl BlockBuilder {
    /// Creates a new [`BlockBuilder`], initializing the block so that it belongs to a microchain.
    ///
    /// Initializes the block so that it belongs to the microchain identified by `chain_id` and
    /// owned by `owner`. It becomes the block after the specified `previous_block`, or the genesis
    /// block if [`None`] is specified.
    ///
    /// # Notes
    ///
    /// This is an internal method, because the [`BlockBuilder`] instance should be built by an
    /// [`ActiveChain`]. External users should only be able to add operations and messages to the
    /// block.
    pub(crate) fn new(
        chain_id: ChainId,
        owner: AccountOwner,
        epoch: Epoch,
        previous_block: Option<&CertifiedBlock>,
        validator: TestValidator,
    ) -> Self {
        let previous_block_hash = previous_block.map(|block| block.certificate.hash());
        let height = previous_block
            .map(|block| {
                block
                    .certificate
                    .inner()
                    .height()
                    .try_add_one()
                    .expect("Block height limit reached")
            })
            .unwrap_or_default();

        BlockBuilder {
            block: ProposedBlock {
                epoch,
                chain_id,
                incoming_bundles: vec![],
                operations: vec![],
                previous_block_hash,
                height,
                authenticated_signer: Some(owner),
                timestamp: Timestamp::from(0),
            },
            validator,
        }
    }

    /// Configures the timestamp of this block.
    pub fn with_timestamp(&mut self, timestamp: Timestamp) -> &mut Self {
        self.block.timestamp = timestamp;
        self
    }

    /// Adds a native token transfer to this block.
    pub fn with_native_token_transfer(
        &mut self,
        sender: AccountOwner,
        recipient: Recipient,
        amount: Amount,
    ) -> &mut Self {
        self.with_system_operation(SystemOperation::Transfer {
            owner: sender,
            recipient,
            amount,
        })
    }

    /// Adds a [`SystemOperation`] to this block.
    pub(crate) fn with_system_operation(&mut self, operation: SystemOperation) -> &mut Self {
        self.block.operations.push(operation.into());
        self
    }

    /// Adds an operation to change this chain's ownership.
    pub fn with_owner_change(
        &mut self,
        super_owners: Vec<AccountOwner>,
        owners: Vec<(AccountOwner, u64)>,
        multi_leader_rounds: u32,
        open_multi_leader_rounds: bool,
        timeout_config: TimeoutConfig,
    ) -> &mut Self {
        self.with_system_operation(SystemOperation::ChangeOwnership {
            super_owners,
            owners,
            multi_leader_rounds,
            open_multi_leader_rounds,
            timeout_config,
        })
    }

    /// Adds an application permissions change to this block.
    pub fn with_change_application_permissions(
        &mut self,
        permissions: ApplicationPermissions,
    ) -> &mut Self {
        self.with_system_operation(SystemOperation::ChangeApplicationPermissions(permissions))
    }

    /// Adds a user `operation` to this block.
    ///
    /// The operation is serialized using [`bcs`] and added to the block, marked to be executed by
    /// `application`.
    pub fn with_operation<Abi>(
        &mut self,
        application_id: ApplicationId<Abi>,
        operation: Abi::Operation,
    ) -> &mut Self
    where
        Abi: ContractAbi,
    {
        let operation = Abi::serialize_operation(&operation)
            .expect("Failed to serialize `Operation` in BlockBuilder");
        self.with_raw_operation(application_id.forget_abi(), operation)
    }

    /// Adds an already serialized user `operation` to this block.
    pub fn with_raw_operation(
        &mut self,
        application_id: ApplicationId,
        operation: impl Into<Vec<u8>>,
    ) -> &mut Self {
        self.block.operations.push(Operation::User {
            application_id,
            bytes: operation.into(),
        });
        self
    }

    /// Receives incoming message bundles by specifying them directly.
    ///
    /// This is an internal method that bypasses the check to see if the messages are already
    /// present in the inboxes of the microchain that owns this block.
    pub(crate) fn with_incoming_bundles(
        &mut self,
        bundles: impl IntoIterator<Item = IncomingBundle>,
    ) -> &mut Self {
        self.block.incoming_bundles.extend(bundles);
        self
    }

    /// Receives all direct messages  that were sent to this chain by the given certificate.
    pub fn with_messages_from(&mut self, certificate: &ConfirmedBlockCertificate) -> &mut Self {
        self.with_messages_from_by_action(certificate, MessageAction::Accept)
    }

    /// Receives all messages that were sent to this chain by the given certificate.
    pub fn with_messages_from_by_action(
        &mut self,
        certificate: &ConfirmedBlockCertificate,
        action: MessageAction,
    ) -> &mut Self {
        let origin = certificate.inner().chain_id();
        let bundles =
            certificate
                .message_bundles_for(self.block.chain_id)
                .map(|(_epoch, bundle)| IncomingBundle {
                    origin,
                    bundle,
                    action,
                });
        self.with_incoming_bundles(bundles)
    }

    /// Tries to sign the prepared block with the [`TestValidator`]'s keys and return the
    /// resulting [`Certificate`]. Returns an error if block execution fails.
    pub(crate) async fn try_sign(self, blobs: &[Blob]) -> Result<CertifiedBlock, WorkerError> {
        let published_blobs = self
            .block
            .published_blob_ids()
            .into_iter()
            .map(|blob_id| {
                blobs
                    .iter()
                    .find(|blob| blob.id() == blob_id)
                    .expect("missing published blob")
                    .clone()
            })
            .collect();
        let (block, resources, _) = self
            .validator
            .worker()
            .stage_block_execution(self.block, None, published_blobs)
            .await?;

        let value = ConfirmedBlock::new(block);
        let vote = LiteVote::new(
            LiteValue::new(&value),
            Round::Fast,
            self.validator.key_pair(),
        );
        let committee = self.validator.committee().await;
        let mut builder = SignatureAggregator::new(value, Round::Fast, &committee);
        let certificate = builder
            .append(vote.public_key, vote.signature)
            .expect("Failed to sign block")
            .expect("Committee has more than one test validator");

        Ok(CertifiedBlock {
            certificate,
            resources,
        })
    }
}

/// A block that has been confirmed and certified by the validators.
#[derive(Clone, Debug)]
pub struct CertifiedBlock {
    pub(super) certificate: ConfirmedBlockCertificate,
    pub(super) resources: ResourceTracker,
}

impl CertifiedBlock {
    /// Returns the header of this [`CertifiedBlock`].
    pub fn header(&self) -> &BlockHeader {
        &self.certificate.inner().block().header
    }

    /// Returns the messages in this [`CertifiedBlock`].
    pub fn messages(&self) -> &[Vec<OutgoingMessage>] {
        self.certificate.inner().block().messages()
    }

    /// Returns the number of messages produced by this [`CertifiedBlock`].
    pub fn outgoing_message_count(&self) -> usize {
        self.certificate.outgoing_message_count()
    }

    /// Returns the [`MessageId`] for the `message_index`th outgoing message produced by the
    /// `operation_index`th operation in this [`CertifiedBlock`].
    pub fn message_id_for_operation(
        &self,
        operation_index: usize,
        message_index: u32,
    ) -> MessageId {
        self.certificate
            .inner()
            .block()
            .message_id_for_operation(operation_index, message_index)
            .expect(
                "Missing {message_index}th outgoing message \
                produced by {operation_index}th operation",
            )
    }

    /// Returns [`true`] if this block used less than the [`Resources`] listed for comparison.
    pub fn consumed_less_than_or_equal_resources(&self, resources: Resources) -> bool {
        let Resources {
            fuel: maximum_fuel,
            read_operations: maximum_read_operations,
            write_operations: maximum_write_operations,
            bytes_to_read,
            bytes_to_write,
            blobs_to_read,
            blobs_to_publish,
            blob_bytes_to_read,
            blob_bytes_to_publish,
            messages: maximum_messages,
            message_size,
            storage_size_delta,
            service_as_oracle_queries,
            http_requests: maximum_http_requests,
        } = resources;

        let ResourceTracker {
            blocks: _,
            block_size: _,
            fuel,
            read_operations,
            write_operations,
            bytes_read,
            bytes_written,
            blobs_read,
            blobs_published,
            blob_bytes_read,
            blob_bytes_published,
            bytes_stored,
            operations: _,
            operation_bytes: _,
            messages,
            message_bytes,
            http_requests,
            service_oracle_queries,
            service_oracle_execution: _,
            grants: _,
        } = self.resources;

        fuel <= maximum_fuel
            && read_operations <= maximum_read_operations
            && write_operations <= maximum_write_operations
            && bytes_read <= bytes_to_read.into()
            && bytes_written <= bytes_to_write.into()
            && blobs_read <= blobs_to_read
            && blobs_published <= blobs_to_publish
            && blob_bytes_read <= blob_bytes_to_read.into()
            && blob_bytes_published <= blob_bytes_to_publish.into()
            && messages <= maximum_messages
            && message_bytes <= message_size.into()
            && bytes_stored <= storage_size_delta.try_into().unwrap_or(i32::MAX)
            && service_oracle_queries <= service_as_oracle_queries
            && http_requests <= maximum_http_requests
    }
}
