// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A builder of [`Block`]s which are then signed to become [`Certificate`]s.
//!
//! Helps with the construction of blocks, adding operations and

use linera_base::{
    crypto::PublicKey,
    data_types::{Amount, ApplicationPermissions, Round, Timestamp},
    identifiers::{ApplicationId, ChainId, GenericApplicationId, Owner},
    ownership::TimeoutConfig,
};
use linera_chain::data_types::{
    Block, Certificate, ChannelFullName, HashedCertificateValue, IncomingBundle, LiteVote, Medium,
    MessageAction, Origin, SignatureAggregator,
};
use linera_execution::{
    system::{Recipient, SystemChannel, SystemOperation, UserData},
    Operation,
};

use super::TestValidator;
use crate::ToBcsBytes;

/// A helper type to build [`Block`]s using the builder pattern, and then signing them into
/// [`Certificate`]s using a [`TestValidator`].
pub struct BlockBuilder {
    block: Block,
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
        previous_block: Option<&Certificate>,
        validator: TestValidator,
    ) -> Self {
        let previous_block_hash = previous_block.map(|certificate| certificate.hash());
        let height = previous_block
            .map(|certificate| {
                certificate
                    .value()
                    .height()
                    .try_add_one()
                    .expect("Block height limit reached")
            })
            .unwrap_or_default();

        BlockBuilder {
            block: Block {
                epoch: 0.into(),
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
            user_data: UserData(None),
        })
    }

    /// Adds a [`SystemOperation`] to this block.
    pub(crate) fn with_system_operation(&mut self, operation: SystemOperation) -> &mut Self {
        self.block.operations.push(operation.into());
        self
    }

    /// Adds a request to register an application on this chain.
    pub fn with_request_for_application<Abi>(
        &mut self,
        application: ApplicationId<Abi>,
    ) -> &mut Self {
        self.with_system_operation(SystemOperation::RequestApplication {
            chain_id: application.creation.chain_id,
            application_id: application.forget_abi(),
        })
    }

    /// Adds an operation to change this chain's ownership.
    pub fn with_owner_change(
        &mut self,
        super_owners: Vec<PublicKey>,
        owners: Vec<(PublicKey, u64)>,
        multi_leader_rounds: u32,
        timeout_config: TimeoutConfig,
    ) -> &mut Self {
        self.with_system_operation(SystemOperation::ChangeOwnership {
            super_owners,
            owners,
            multi_leader_rounds,
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
        operation: impl ToBcsBytes,
    ) -> &mut Self {
        self.block.operations.push(Operation::User {
            application_id: application_id.forget_abi(),
            bytes: operation
                .to_bcs_bytes()
                .expect("Failed to serialize operation"),
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
        certificate: &Certificate,
        channel: SystemChannel,
    ) -> &mut Self {
        let medium = Medium::Channel(ChannelFullName {
            application_id: GenericApplicationId::System,
            name: channel.name(),
        });
        self.with_messages_from_by_medium(certificate, &medium, MessageAction::Accept)
    }

    /// Receives all direct messages  that were sent to this chain by the given certificate.
    pub fn with_messages_from(&mut self, certificate: &Certificate) -> &mut Self {
        self.with_messages_from_by_medium(certificate, &Medium::Direct, MessageAction::Accept)
    }

    /// Receives all messages that were sent to this chain by the given certificate.
    pub fn with_messages_from_by_medium(
        &mut self,
        certificate: &Certificate,
        medium: &Medium,
        action: MessageAction,
    ) -> &mut Self {
        let origin = Origin {
            sender: certificate.value().chain_id(),
            medium: medium.clone(),
        };
        let bundles = certificate
            .message_bundles_for(medium, self.block.chain_id)
            .into_iter()
            .map(|(_epoch, bundle)| IncomingBundle {
                origin: origin.clone(),
                bundle,
                action,
            });
        self.with_incoming_bundles(bundles)
    }

    /// Tries to sign the prepared [`Block`] with the [`TestValidator`]'s keys and return the
    /// resulting [`Certificate`]. Returns an error if block execution fails.
    pub(crate) async fn try_sign(self) -> anyhow::Result<Certificate> {
        let (executed_block, _) = self
            .validator
            .worker()
            .stage_block_execution(self.block)
            .await?;

        let value = HashedCertificateValue::new_confirmed(executed_block);
        let vote = LiteVote::new(value.lite(), Round::Fast, self.validator.key_pair());
        let mut builder = SignatureAggregator::new(value, Round::Fast, self.validator.committee());
        let certificate = builder
            .append(vote.validator, vote.signature)
            .expect("Failed to sign block")
            .expect("Committee has more than one test validator");

        Ok(certificate)
    }
}
