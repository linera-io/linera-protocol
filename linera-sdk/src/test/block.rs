// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A builder of [`Block`]s which are then signed to become [`Certificate`]s.
//!
//! Helps with the construction of blocks, adding operations and

use linera_base::{
    abi::ContractAbi,
    data_types::{Amount, ApplicationPermissions, Blob, Round, Timestamp},
    hashed::Hashed,
    identifiers::{ApplicationId, ChainId, ChannelFullName, GenericApplicationId, Owner},
    ownership::TimeoutConfig,
};
use linera_chain::{
    data_types::{
        IncomingBundle, LiteValue, LiteVote, Medium, MessageAction, Origin, ProposedBlock,
        SignatureAggregator,
    },
    types::{ConfirmedBlock, ConfirmedBlockCertificate},
};
use linera_execution::{
    committee::Epoch,
    system::{Recipient, SystemChannel, SystemOperation},
    Operation,
};

use super::TestValidator;
use crate::ToBcsBytes;

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
        owner: Owner,
        epoch: Epoch,
        previous_block: Option<&ConfirmedBlockCertificate>,
        validator: TestValidator,
    ) -> Self {
        let previous_block_hash = previous_block.map(|certificate| certificate.hash());
        let height = previous_block
            .map(|certificate| {
                certificate
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
        sender: Option<Owner>,
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
        super_owners: Vec<Owner>,
        owners: Vec<(Owner, u64)>,
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
        self.with_raw_operation(
            application_id.forget_abi(),
            operation
                .to_bcs_bytes()
                .expect("Failed to serialize operation"),
        )
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

    /// Receives all admin messages that were sent to this chain by the given certificate.
    pub fn with_system_messages_from(
        &mut self,
        certificate: &ConfirmedBlockCertificate,
        channel: SystemChannel,
    ) -> &mut Self {
        let medium = Medium::Channel(ChannelFullName {
            application_id: GenericApplicationId::System,
            name: channel.name(),
        });
        self.with_messages_from_by_medium(certificate, &medium, MessageAction::Accept)
    }

    /// Receives all direct messages  that were sent to this chain by the given certificate.
    pub fn with_messages_from(&mut self, certificate: &ConfirmedBlockCertificate) -> &mut Self {
        self.with_messages_from_by_medium(certificate, &Medium::Direct, MessageAction::Accept)
    }

    /// Receives all messages that were sent to this chain by the given certificate.
    pub fn with_messages_from_by_medium(
        &mut self,
        certificate: &ConfirmedBlockCertificate,
        medium: &Medium,
        action: MessageAction,
    ) -> &mut Self {
        let origin = Origin {
            sender: certificate.inner().chain_id(),
            medium: medium.clone(),
        };
        let bundles = certificate
            .message_bundles_for(medium, self.block.chain_id)
            .map(|(_epoch, bundle)| IncomingBundle {
                origin: origin.clone(),
                bundle,
                action,
            });
        self.with_incoming_bundles(bundles)
    }

    /// Tries to sign the prepared block with the [`TestValidator`]'s keys and return the
    /// resulting [`Certificate`]. Returns an error if block execution fails.
    pub(crate) async fn try_sign(
        self,
        blobs: &[Blob],
    ) -> anyhow::Result<ConfirmedBlockCertificate> {
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
        let (executed_block, _) = self
            .validator
            .worker()
            .stage_block_execution(self.block, None, published_blobs)
            .await?;

        let value = Hashed::new(ConfirmedBlock::new(executed_block));
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

        Ok(certificate)
    }
}
