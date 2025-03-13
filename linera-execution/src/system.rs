// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
#[path = "./unit_tests/system_tests.rs"]
mod tests;

#[cfg(with_metrics)]
use std::sync::LazyLock;
use std::{
    collections::{BTreeMap, HashSet},
    fmt::{self, Display, Formatter},
    mem,
};

use async_graphql::Enum;
use custom_debug_derive::Debug;
use linera_base::{
    crypto::CryptoHash,
    data_types::{
        Amount, ApplicationPermissions, Blob, BlobContent, BlockHeight, OracleResponse, Timestamp,
    },
    ensure, hex_debug,
    identifiers::{
        Account, AccountOwner, BlobId, BlobType, ChainDescription, ChainId, ChannelFullName,
        EventId, MessageId, ModuleId, Owner, StreamId,
    },
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_views::{
    context::Context,
    map_view::HashedMapView,
    register_view::HashedRegisterView,
    set_view::HashedSetView,
    views::{ClonableView, HashableView, View},
};
use serde::{Deserialize, Serialize};
#[cfg(with_metrics)]
use {linera_base::prometheus_util::register_int_counter_vec, prometheus::IntCounterVec};

#[cfg(test)]
use crate::test_utils::SystemExecutionState;
use crate::{
    committee::{Committee, Epoch},
    ChannelName, ChannelSubscription, Destination, ExecutionError, ExecutionRuntimeContext,
    MessageContext, MessageKind, OperationContext, QueryContext, QueryOutcome, RawExecutionOutcome,
    RawOutgoingMessage, ResourceController, TransactionTracker, UserApplicationDescription,
    UserApplicationId,
};

/// The relative index of the `OpenChain` message created by the `OpenChain` operation.
pub static OPEN_CHAIN_MESSAGE_INDEX: u32 = 0;
/// The event stream name for new epochs and committees.
pub static EPOCH_STREAM_NAME: &[u8] = &[0];
/// The event stream name for removed epochs.
pub static REMOVED_EPOCH_STREAM_NAME: &[u8] = &[1];

/// The number of times the [`SystemOperation::OpenChain`] was executed.
#[cfg(with_metrics)]
static OPEN_CHAIN_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "open_chain_count",
        "The number of times the `OpenChain` operation was executed",
        &[],
    )
});

/// A view accessing the execution state of the system of a chain.
#[derive(Debug, ClonableView, HashableView)]
pub struct SystemExecutionStateView<C> {
    /// How the chain was created. May be unknown for inactive chains.
    pub description: HashedRegisterView<C, Option<ChainDescription>>,
    /// The number identifying the current configuration.
    pub epoch: HashedRegisterView<C, Option<Epoch>>,
    /// The admin of the chain.
    pub admin_id: HashedRegisterView<C, Option<ChainId>>,
    /// Track the channels that we have subscribed to.
    pub subscriptions: HashedSetView<C, ChannelSubscription>,
    /// The committees that we trust, indexed by epoch number.
    // Not using a `MapView` because the set active of committees is supposed to be
    // small. Plus, currently, we would create the `BTreeMap` anyway in various places
    // (e.g. the `OpenChain` operation).
    pub committees: HashedRegisterView<C, BTreeMap<Epoch, Committee>>,
    /// Ownership of the chain.
    pub ownership: HashedRegisterView<C, ChainOwnership>,
    /// Balance of the chain. (Available to any user able to create blocks in the chain.)
    pub balance: HashedRegisterView<C, Amount>,
    /// Balances attributed to a given owner.
    pub balances: HashedMapView<C, AccountOwner, Amount>,
    /// The timestamp of the most recent block.
    pub timestamp: HashedRegisterView<C, Timestamp>,
    /// Whether this chain has been closed.
    pub closed: HashedRegisterView<C, bool>,
    /// Permissions for applications on this chain.
    pub application_permissions: HashedRegisterView<C, ApplicationPermissions>,
    /// Blobs that have been used or published on this chain.
    pub used_blobs: HashedSetView<C, BlobId>,
}

/// The configuration for a new chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct OpenChainConfig {
    pub ownership: ChainOwnership,
    pub admin_id: ChainId,
    pub epoch: Epoch,
    pub committees: BTreeMap<Epoch, Committee>,
    pub balance: Amount,
    pub application_permissions: ApplicationPermissions,
}

/// A system operation.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum SystemOperation {
    /// Transfers `amount` units of value from the given owner's account to the recipient.
    /// If no owner is given, try to take the units out of the unattributed account.
    Transfer {
        #[debug(skip_if = Option::is_none)]
        owner: Option<Owner>,
        recipient: Recipient,
        amount: Amount,
    },
    /// Claims `amount` units of value from the given owner's account in the remote
    /// `target` chain. Depending on its configuration, the `target` chain may refuse to
    /// process the message.
    Claim {
        owner: Owner,
        target_id: ChainId,
        recipient: Recipient,
        amount: Amount,
    },
    /// Creates (or activates) a new chain.
    /// This will automatically subscribe to the future committees created by `admin_id`.
    OpenChain(OpenChainConfig),
    /// Closes the chain.
    CloseChain,
    /// Changes the ownership of the chain.
    ChangeOwnership {
        /// Super owners can propose fast blocks in the first round, and regular blocks in any round.
        #[debug(skip_if = Vec::is_empty)]
        super_owners: Vec<Owner>,
        /// The regular owners, with their weights that determine how often they are round leader.
        #[debug(skip_if = Vec::is_empty)]
        owners: Vec<(Owner, u64)>,
        /// The number of initial rounds after 0 in which all owners are allowed to propose blocks.
        multi_leader_rounds: u32,
        /// Whether the multi-leader rounds are unrestricted, i.e. not limited to chain owners.
        /// This should only be `true` on chains with restrictive application permissions and an
        /// application-based mechanism to select block proposers.
        open_multi_leader_rounds: bool,
        /// The timeout configuration: how long fast, multi-leader and single-leader rounds last.
        timeout_config: TimeoutConfig,
    },
    /// Changes the application permissions configuration on this chain.
    ChangeApplicationPermissions(ApplicationPermissions),
    /// Subscribes to a system channel.
    Subscribe {
        chain_id: ChainId,
        channel: SystemChannel,
    },
    /// Unsubscribes from a system channel.
    Unsubscribe {
        chain_id: ChainId,
        channel: SystemChannel,
    },
    /// Publishes a new application module.
    PublishModule { module_id: ModuleId },
    /// Publishes a new committee as a blob. This can be assigned to an epoch using
    /// [`AdminOperation::CreateCommittee`] in a later block.
    PublishCommitteeBlob { blob_hash: CryptoHash },
    /// Publishes a new data blob.
    PublishDataBlob { blob_hash: CryptoHash },
    /// Reads a blob and discards the result.
    // TODO(#2490): Consider removing this.
    ReadBlob { blob_id: BlobId },
    /// Creates a new application.
    CreateApplication {
        module_id: ModuleId,
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        parameters: Vec<u8>,
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug", skip_if = Vec::is_empty)]
        instantiation_argument: Vec<u8>,
        #[debug(skip_if = Vec::is_empty)]
        required_application_ids: Vec<UserApplicationId>,
    },
    /// Operations that are only allowed on the admin chain.
    Admin(AdminOperation),
    /// Processes an event about a new epoch and committee.
    ProcessNewEpoch(Epoch),
    /// Processes an event about a removed epoch and committee.
    ProcessRemovedEpoch(Epoch),
}

/// Operations that are only allowed on the admin chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum AdminOperation {
    /// Registers a new committee. Other chains can then migrate to the new epoch by executing
    /// [`SystemOperation::ProcessNewEpoch`].
    CreateCommittee { epoch: Epoch, blob_hash: CryptoHash },
    /// Removes a committee. Other chains should execute [`SystemOperation::ProcessRemovedEpoch`],
    /// so that blocks from the retired epoch will not be accepted until they are followed (hence
    /// re-certified) by a block certified by a recent committee.
    RemoveCommittee { epoch: Epoch },
}

/// A system message meant to be executed on a remote chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum SystemMessage {
    /// Credits `amount` units of value to the account `target` -- unless the message is
    /// bouncing, in which case `source` is credited instead.
    Credit {
        #[debug(skip_if = Option::is_none)]
        target: Option<AccountOwner>,
        amount: Amount,
        #[debug(skip_if = Option::is_none)]
        source: Option<AccountOwner>,
    },
    /// Withdraws `amount` units of value from the account and starts a transfer to credit
    /// the recipient. The message must be properly authenticated. Receiver chains may
    /// refuse it depending on their configuration.
    Withdraw {
        owner: AccountOwner,
        amount: Amount,
        recipient: Recipient,
    },
    /// Creates (or activates) a new chain.
    OpenChain(OpenChainConfig),
    /// Subscribes to a channel.
    Subscribe {
        id: ChainId,
        subscription: ChannelSubscription,
    },
    /// Unsubscribes from a channel.
    Unsubscribe {
        id: ChainId,
        subscription: ChannelSubscription,
    },
    /// Notifies that a new application was created.
    ApplicationCreated,
}

/// A query to the system state.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct SystemQuery;

/// The response to a system query.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct SystemResponse {
    pub chain_id: ChainId,
    pub balance: Amount,
}

/// The channels available in the system application.
#[derive(
    Enum, Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize, clap::ValueEnum,
)]
pub enum SystemChannel {
    /// Channel used to broadcast reconfigurations.
    Admin,
}

impl SystemChannel {
    /// The [`ChannelName`] of this [`SystemChannel`].
    pub fn name(&self) -> ChannelName {
        bcs::to_bytes(self)
            .expect("`SystemChannel` can be serialized")
            .into()
    }

    /// The [`ChannelFullName`] of this [`SystemChannel`].
    pub fn full_name(&self) -> ChannelFullName {
        ChannelFullName::system(self.name())
    }
}

impl Display for SystemChannel {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        let display_name = match self {
            SystemChannel::Admin => "Admin",
        };

        write!(formatter, "{display_name}")
    }
}

/// The recipient of a transfer.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize)]
pub enum Recipient {
    /// This is mainly a placeholder for future extensions.
    Burn,
    /// Transfers to the balance of the given account.
    Account(Account),
}

impl Recipient {
    /// Returns the default recipient for the given chain (no owner).
    pub fn chain(chain_id: ChainId) -> Recipient {
        Recipient::Account(Account::chain(chain_id))
    }

    /// Returns the default recipient for the root chain with the given index.
    #[cfg(with_testing)]
    pub fn root(index: u32) -> Recipient {
        Recipient::chain(ChainId::root(index))
    }
}

/// Optional user message attached to a transfer.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Hash, Default, Debug, Serialize, Deserialize)]
pub struct UserData(pub Option<[u8; 32]>);

impl UserData {
    pub fn from_option_string(opt_str: Option<String>) -> Result<Self, usize> {
        // Convert the Option<String> to Option<[u8; 32]>
        let option_array = match opt_str {
            Some(s) => {
                // Convert the String to a Vec<u8>
                let vec = s.into_bytes();
                if vec.len() <= 32 {
                    // Create an array from the Vec<u8>
                    let mut array = [b' '; 32];

                    // Copy bytes from the vector into the array
                    let len = vec.len().min(32);
                    array[..len].copy_from_slice(&vec[..len]);

                    Some(array)
                } else {
                    return Err(vec.len());
                }
            }
            None => None,
        };

        // Return the UserData with the converted Option<[u8; 32]>
        Ok(UserData(option_array))
    }
}

#[derive(Debug)]
pub struct CreateApplicationResult {
    pub app_id: UserApplicationId,
    pub txn_tracker: TransactionTracker,
}

impl<C> SystemExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    /// Invariant for the states of active chains.
    pub fn is_active(&self) -> bool {
        self.description.get().is_some()
            && self.ownership.get().is_active()
            && self.current_committee().is_some()
            && self.admin_id.get().is_some()
    }

    /// Returns the current committee, if any.
    pub fn current_committee(&self) -> Option<(Epoch, &Committee)> {
        let epoch = self.epoch.get().as_ref()?;
        let committee = self.committees.get().get(epoch)?;
        Some((*epoch, committee))
    }

    /// Executes the sender's side of an operation and returns a list of actions to be
    /// taken.
    pub async fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: SystemOperation,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<Owner>>,
    ) -> Result<Option<(UserApplicationId, Vec<u8>)>, ExecutionError> {
        use SystemOperation::*;
        let mut outcome = RawExecutionOutcome {
            authenticated_signer: context.authenticated_signer,
            refund_grant_to: context.refund_grant_to(),
            ..RawExecutionOutcome::default()
        };
        let mut new_application = None;
        match operation {
            OpenChain(config) => {
                let next_message_id = context.next_message_id(txn_tracker.next_message_index());
                let message = self.open_chain(config, next_message_id).await?;
                outcome.messages.push(message);
                #[cfg(with_metrics)]
                OPEN_CHAIN_COUNT.with_label_values(&[]).inc();
            }
            ChangeOwnership {
                super_owners,
                owners,
                multi_leader_rounds,
                open_multi_leader_rounds,
                timeout_config,
            } => {
                self.ownership.set(ChainOwnership {
                    super_owners: super_owners.into_iter().collect(),
                    owners: owners.into_iter().collect(),
                    multi_leader_rounds,
                    open_multi_leader_rounds,
                    timeout_config,
                });
            }
            ChangeApplicationPermissions(application_permissions) => {
                self.application_permissions.set(application_permissions);
            }
            CloseChain => {
                let messages = self.close_chain(context.chain_id).await?;
                outcome.messages.extend(messages);
            }
            Transfer {
                owner,
                amount,
                recipient,
                ..
            } => {
                let message = self
                    .transfer(
                        context.authenticated_signer,
                        None,
                        owner.map(AccountOwner::User),
                        recipient,
                        amount,
                    )
                    .await?;

                if let Some(message) = message {
                    outcome.messages.push(message)
                }
            }
            Claim {
                owner,
                target_id,
                recipient,
                amount,
            } => {
                let message = self
                    .claim(
                        context.authenticated_signer,
                        None,
                        AccountOwner::User(owner),
                        target_id,
                        recipient,
                        amount,
                    )
                    .await?;

                outcome.messages.push(message)
            }
            Admin(admin_operation) => {
                ensure!(
                    *self.admin_id.get() == Some(context.chain_id),
                    ExecutionError::AdminOperationOnNonAdminChain
                );
                match admin_operation {
                    AdminOperation::CreateCommittee { epoch, blob_hash } => {
                        self.check_next_epoch(epoch)?;
                        let blob_id = BlobId::new(blob_hash, BlobType::Committee);
                        let content = self.read_blob_content(blob_id).await?;
                        let committee = bcs::from_bytes(content.bytes())?;
                        // Not passing in the resource controller: blob was published, not read.
                        self.blob_used(Some(txn_tracker), None, blob_id, content.bytes().len())
                            .await?;
                        self.committees.get_mut().insert(epoch, committee);
                        self.epoch.set(Some(epoch));
                        txn_tracker.add_event(
                            StreamId::system(EPOCH_STREAM_NAME),
                            bcs::to_bytes(&epoch)?,
                            bcs::to_bytes(&blob_hash)?,
                        );
                    }
                    AdminOperation::RemoveCommittee { epoch } => {
                        ensure!(
                            self.committees.get_mut().remove(&epoch).is_some(),
                            ExecutionError::InvalidCommitteeRemoval
                        );
                        txn_tracker.add_event(
                            StreamId::system(REMOVED_EPOCH_STREAM_NAME),
                            bcs::to_bytes(&epoch)?,
                            vec![],
                        );
                    }
                }
            }
            Subscribe { chain_id, channel } => {
                ensure!(
                    context.chain_id != chain_id,
                    ExecutionError::SelfSubscription(context.chain_id, channel)
                );
                let subscription = ChannelSubscription {
                    chain_id,
                    name: channel.name(),
                };
                ensure!(
                    !self.subscriptions.contains(&subscription).await?,
                    ExecutionError::AlreadySubscribedToChannel(context.chain_id, channel)
                );
                self.subscriptions.insert(&subscription)?;
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(chain_id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Protected,
                    message: SystemMessage::Subscribe {
                        id: context.chain_id,
                        subscription,
                    },
                };
                outcome.messages.push(message);
            }
            Unsubscribe { chain_id, channel } => {
                let subscription = ChannelSubscription {
                    chain_id,
                    name: channel.name(),
                };
                ensure!(
                    self.subscriptions.contains(&subscription).await?,
                    ExecutionError::InvalidUnsubscription(context.chain_id, channel)
                );
                self.subscriptions.remove(&subscription)?;
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(chain_id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Protected,
                    message: SystemMessage::Unsubscribe {
                        id: context.chain_id,
                        subscription,
                    },
                };
                outcome.messages.push(message);
            }
            PublishModule { module_id } => {
                self.blob_published(&BlobId::new(
                    module_id.contract_blob_hash,
                    BlobType::ContractBytecode,
                ))?;
                self.blob_published(&BlobId::new(
                    module_id.service_blob_hash,
                    BlobType::ServiceBytecode,
                ))?;
            }
            CreateApplication {
                module_id,
                parameters,
                instantiation_argument,
                required_application_ids,
            } => {
                let txn_tracker_moved = mem::take(txn_tracker);
                let CreateApplicationResult {
                    app_id,
                    txn_tracker: txn_tracker_moved,
                } = self
                    .create_application(
                        context.chain_id,
                        context.height,
                        module_id,
                        parameters,
                        required_application_ids,
                        txn_tracker_moved,
                        Some(resource_controller),
                    )
                    .await?;
                *txn_tracker = txn_tracker_moved;
                new_application = Some((app_id, instantiation_argument));
            }
            PublishDataBlob { blob_hash } => {
                self.blob_published(&BlobId::new(blob_hash, BlobType::Data))?;
            }
            PublishCommitteeBlob { blob_hash } => {
                self.blob_published(&BlobId::new(blob_hash, BlobType::Committee))?;
            }
            ReadBlob { blob_id } => {
                let content = self.read_blob_content(blob_id).await?;
                self.blob_used(
                    Some(txn_tracker),
                    Some(resource_controller),
                    blob_id,
                    content.bytes().len(),
                )
                .await?;
            }
            ProcessNewEpoch(epoch) => {
                self.check_next_epoch(epoch)?;
                let admin_id = self
                    .admin_id
                    .get()
                    .ok_or_else(|| ExecutionError::InactiveChain)?;
                let event_id = EventId {
                    chain_id: admin_id,
                    stream_id: StreamId::system(EPOCH_STREAM_NAME),
                    key: bcs::to_bytes(&epoch)?,
                };
                let bytes = match txn_tracker.next_replayed_oracle_response()? {
                    None => self.context().extra().get_event(event_id.clone()).await?,
                    Some(OracleResponse::Event(recorded_event_id, bytes))
                        if recorded_event_id == event_id =>
                    {
                        bytes
                    }
                    Some(_) => return Err(ExecutionError::OracleResponseMismatch),
                };
                let blob_id = BlobId::new(bcs::from_bytes(&bytes)?, BlobType::Committee);
                txn_tracker.add_oracle_response(OracleResponse::Event(event_id, bytes));
                let content = self.read_blob_content(blob_id).await?;
                let committee = bcs::from_bytes(content.bytes())?;
                self.blob_used(
                    Some(txn_tracker),
                    Some(resource_controller),
                    blob_id,
                    content.bytes().len(),
                )
                .await?;
                self.committees.get_mut().insert(epoch, committee);
                self.epoch.set(Some(epoch));
            }
            ProcessRemovedEpoch(epoch) => {
                ensure!(
                    self.committees.get_mut().remove(&epoch).is_some(),
                    ExecutionError::InvalidCommitteeRemoval
                );
                let admin_id = self
                    .admin_id
                    .get()
                    .ok_or_else(|| ExecutionError::InactiveChain)?;
                let event_id = EventId {
                    chain_id: admin_id,
                    stream_id: StreamId::system(REMOVED_EPOCH_STREAM_NAME),
                    key: bcs::to_bytes(&epoch)?,
                };
                let bytes = match txn_tracker.next_replayed_oracle_response()? {
                    None => self.context().extra().get_event(event_id.clone()).await?,
                    Some(OracleResponse::Event(recorded_event_id, bytes))
                        if recorded_event_id == event_id =>
                    {
                        bytes
                    }
                    Some(_) => return Err(ExecutionError::OracleResponseMismatch),
                };
                txn_tracker.add_oracle_response(OracleResponse::Event(event_id, bytes));
            }
        }

        txn_tracker.add_system_outcome(outcome)?;
        Ok(new_application)
    }

    /// Returns an error if the `provided` epoch is not exactly one higher than the chain's current
    /// epoch.
    fn check_next_epoch(&self, provided: Epoch) -> Result<(), ExecutionError> {
        let expected = self.epoch.get().expect("chain is active").try_add_one()?;
        ensure!(
            provided == expected,
            ExecutionError::InvalidCommitteeEpoch { provided, expected }
        );
        Ok(())
    }

    pub async fn transfer(
        &mut self,
        authenticated_signer: Option<Owner>,
        authenticated_application_id: Option<UserApplicationId>,
        source: Option<AccountOwner>,
        recipient: Recipient,
        amount: Amount,
    ) -> Result<Option<RawOutgoingMessage<SystemMessage, Amount>>, ExecutionError> {
        match (source, authenticated_signer, authenticated_application_id) {
            (Some(AccountOwner::User(owner)), Some(signer), _) => ensure!(
                signer == owner,
                ExecutionError::UnauthenticatedTransferOwner
            ),
            (
                Some(AccountOwner::Application(account_application)),
                _,
                Some(authorized_application),
            ) => ensure!(
                account_application == authorized_application,
                ExecutionError::UnauthenticatedTransferOwner
            ),
            (None, Some(signer), _) => ensure!(
                self.ownership.get().verify_owner(&signer),
                ExecutionError::UnauthenticatedTransferOwner
            ),
            (_, _, _) => return Err(ExecutionError::UnauthenticatedTransferOwner),
        }
        ensure!(
            amount > Amount::ZERO,
            ExecutionError::IncorrectTransferAmount
        );
        self.debit(source.as_ref(), amount).await?;
        match recipient {
            Recipient::Account(account) => {
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(account.chain_id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Tracked,
                    message: SystemMessage::Credit {
                        amount,
                        source,
                        target: account.owner,
                    },
                };

                Ok(Some(message))
            }
            Recipient::Burn => Ok(None),
        }
    }

    pub async fn claim(
        &self,
        authenticated_signer: Option<Owner>,
        authenticated_application_id: Option<UserApplicationId>,
        source: AccountOwner,
        target_id: ChainId,
        recipient: Recipient,
        amount: Amount,
    ) -> Result<RawOutgoingMessage<SystemMessage, Amount>, ExecutionError> {
        match source {
            AccountOwner::User(owner) => ensure!(
                authenticated_signer == Some(owner),
                ExecutionError::UnauthenticatedClaimOwner
            ),
            AccountOwner::Application(owner) => ensure!(
                authenticated_application_id == Some(owner),
                ExecutionError::UnauthenticatedClaimOwner
            ),
        }
        ensure!(amount > Amount::ZERO, ExecutionError::IncorrectClaimAmount);

        Ok(RawOutgoingMessage {
            destination: Destination::Recipient(target_id),
            authenticated: true,
            grant: Amount::ZERO,
            kind: MessageKind::Simple,
            message: SystemMessage::Withdraw {
                amount,
                owner: source,
                recipient,
            },
        })
    }

    /// Debits an [`Amount`] of tokens from an account's balance.
    async fn debit(
        &mut self,
        account: Option<&AccountOwner>,
        amount: Amount,
    ) -> Result<(), ExecutionError> {
        let balance = if let Some(owner) = account {
            self.balances.get_mut(owner).await?.ok_or_else(|| {
                ExecutionError::InsufficientFunding {
                    balance: Amount::ZERO,
                }
            })?
        } else {
            self.balance.get_mut()
        };

        balance
            .try_sub_assign(amount)
            .map_err(|_| ExecutionError::InsufficientFunding { balance: *balance })?;

        if let Some(owner) = account {
            if balance.is_zero() {
                self.balances.remove(owner)?;
            }
        }

        Ok(())
    }

    /// Executes a cross-chain message that represents the recipient's side of an operation.
    pub async fn execute_message(
        &mut self,
        context: MessageContext,
        message: SystemMessage,
    ) -> Result<RawExecutionOutcome<SystemMessage>, ExecutionError> {
        let mut outcome = RawExecutionOutcome::default();
        use SystemMessage::*;
        match message {
            Credit {
                amount,
                source,
                target,
            } => {
                let receiver = if context.is_bouncing { source } else { target };
                match receiver {
                    None => {
                        let new_balance = self.balance.get().saturating_add(amount);
                        self.balance.set(new_balance);
                    }
                    Some(owner) => {
                        let balance = self.balances.get_mut_or_default(&owner).await?;
                        *balance = balance.saturating_add(amount);
                    }
                }
            }
            Withdraw {
                amount,
                owner,
                recipient,
            } => {
                self.debit(Some(&owner), amount).await?;
                match recipient {
                    Recipient::Account(account) => {
                        let message = RawOutgoingMessage {
                            destination: Destination::Recipient(account.chain_id),
                            authenticated: false,
                            grant: Amount::ZERO,
                            kind: MessageKind::Tracked,
                            message: SystemMessage::Credit {
                                amount,
                                source: Some(owner),
                                target: account.owner,
                            },
                        };
                        outcome.messages.push(message);
                    }
                    Recipient::Burn => (),
                }
            }
            // These messages are executed immediately when cross-chain requests are received.
            Subscribe { .. } | Unsubscribe { .. } | OpenChain(_) => {}
            // This message is only a placeholder: Its ID is part of the application ID.
            ApplicationCreated => {}
        }
        Ok(outcome)
    }

    /// Initializes the system application state on a newly opened chain.
    pub fn initialize_chain(
        &mut self,
        message_id: MessageId,
        timestamp: Timestamp,
        config: OpenChainConfig,
    ) {
        // Guaranteed under BFT assumptions.
        assert!(self.description.get().is_none());
        assert!(!self.ownership.get().is_active());
        assert!(self.committees.get().is_empty());
        let OpenChainConfig {
            ownership,
            admin_id,
            epoch,
            committees,
            balance,
            application_permissions,
        } = config;
        let description = ChainDescription::Child(message_id);
        self.description.set(Some(description));
        self.epoch.set(Some(epoch));
        self.committees.set(committees);
        self.admin_id.set(Some(admin_id));
        self.ownership.set(ownership);
        self.timestamp.set(timestamp);
        self.balance.set(balance);
        self.application_permissions.set(application_permissions);
    }

    pub async fn handle_query(
        &mut self,
        context: QueryContext,
        _query: SystemQuery,
    ) -> Result<QueryOutcome<SystemResponse>, ExecutionError> {
        let response = SystemResponse {
            chain_id: context.chain_id,
            balance: *self.balance.get(),
        };
        Ok(QueryOutcome {
            response,
            operations: vec![],
        })
    }

    /// Returns the messages to open a new chain, and subtracts the new chain's balance
    /// from this chain's.
    pub async fn open_chain(
        &mut self,
        config: OpenChainConfig,
        next_message_id: MessageId,
    ) -> Result<RawOutgoingMessage<SystemMessage, Amount>, ExecutionError> {
        let child_id = ChainId::child(next_message_id);
        ensure!(
            self.admin_id.get().as_ref() == Some(&config.admin_id),
            ExecutionError::InvalidNewChainAdminId(child_id)
        );
        ensure!(
            self.committees.get() == &config.committees,
            ExecutionError::InvalidCommittees
        );
        ensure!(
            self.epoch.get().as_ref() == Some(&config.epoch),
            ExecutionError::InvalidEpoch {
                chain_id: child_id,
                epoch: config.epoch,
            }
        );
        self.debit(None, config.balance).await?;
        let open_chain_message = RawOutgoingMessage {
            destination: Destination::Recipient(child_id),
            authenticated: false,
            grant: Amount::ZERO,
            kind: MessageKind::Protected,
            message: SystemMessage::OpenChain(config),
        };
        Ok(open_chain_message)
    }

    pub async fn close_chain(
        &mut self,
        id: ChainId,
    ) -> Result<Vec<RawOutgoingMessage<SystemMessage, Amount>>, ExecutionError> {
        let mut messages = Vec::new();
        // Unsubscribe from all channels.
        self.subscriptions
            .for_each_index(|subscription| {
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(subscription.chain_id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Protected,
                    message: SystemMessage::Unsubscribe { id, subscription },
                };
                messages.push(message);
                Ok(())
            })
            .await?;
        self.subscriptions.clear();
        self.closed.set(true);
        Ok(messages)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_application(
        &mut self,
        chain_id: ChainId,
        block_height: BlockHeight,
        module_id: ModuleId,
        parameters: Vec<u8>,
        required_application_ids: Vec<UserApplicationId>,
        mut txn_tracker: TransactionTracker,
        mut resource_controller: Option<&mut ResourceController<Option<Owner>>>,
    ) -> Result<CreateApplicationResult, ExecutionError> {
        let application_index = txn_tracker.next_application_index();

        let (contract_blob, service_blob) = self.check_bytecode_blobs(&module_id).await?;
        // We only remember to register the blobs that aren't recorded in `used_blobs`
        // already.
        self.blob_used(
            Some(&mut txn_tracker),
            resource_controller.as_deref_mut(),
            contract_blob.id(),
            contract_blob.bytes().len(),
        )
        .await?;
        self.blob_used(
            Some(&mut txn_tracker),
            resource_controller.as_deref_mut(),
            service_blob.id(),
            service_blob.bytes().len(),
        )
        .await?;

        let application_description = UserApplicationDescription {
            module_id,
            creator_chain_id: chain_id,
            block_height,
            application_index,
            parameters,
            required_application_ids,
        };
        self.check_required_applications(
            &application_description,
            Some(&mut txn_tracker),
            resource_controller,
        )
        .await?;

        txn_tracker.add_created_blob(Blob::new_application_description(&application_description));

        Ok(CreateApplicationResult {
            app_id: UserApplicationId::from(&application_description),
            txn_tracker,
        })
    }

    async fn check_required_applications(
        &mut self,
        application_description: &UserApplicationDescription,
        mut txn_tracker: Option<&mut TransactionTracker>,
        mut resource_controller: Option<&mut ResourceController<Option<Owner>>>,
    ) -> Result<(), ExecutionError> {
        // Make sure that referenced applications IDs have been registered.
        for required_id in &application_description.required_application_ids {
            Box::pin(self.describe_application(
                *required_id,
                txn_tracker.as_deref_mut(),
                resource_controller.as_deref_mut(),
            ))
            .await?;
        }
        Ok(())
    }

    /// Retrieves an application's description.
    // False positive? https://github.com/rust-lang/rust-clippy/issues/14148
    #[expect(clippy::needless_option_as_deref)]
    pub async fn describe_application(
        &mut self,
        id: UserApplicationId,
        mut txn_tracker: Option<&mut TransactionTracker>,
        mut resource_controller: Option<&mut ResourceController<Option<Owner>>>,
    ) -> Result<UserApplicationDescription, ExecutionError> {
        let blob_id = id.description_blob_id();
        let blob_content = match txn_tracker
            .as_ref()
            .and_then(|tracker| tracker.created_blobs().get(&blob_id))
        {
            Some(blob) => blob.content().clone(),
            None => self.read_blob_content(blob_id).await?,
        };
        self.blob_used(
            txn_tracker.as_deref_mut(),
            resource_controller.as_deref_mut(),
            blob_id,
            blob_content.bytes().len(),
        )
        .await?;
        let description: UserApplicationDescription = bcs::from_bytes(blob_content.bytes())?;

        let (contract_blob, service_blob) =
            self.check_bytecode_blobs(&description.module_id).await?;
        // We only remember to register the blobs that aren't recorded in `used_blobs`
        // already.
        self.blob_used(
            txn_tracker.as_deref_mut(),
            resource_controller.as_deref_mut(),
            contract_blob.id(),
            contract_blob.bytes().len(),
        )
        .await?;
        self.blob_used(
            txn_tracker.as_deref_mut(),
            resource_controller.as_deref_mut(),
            service_blob.id(),
            service_blob.bytes().len(),
        )
        .await?;

        self.check_required_applications(
            &description,
            txn_tracker,
            resource_controller.as_deref_mut(),
        )
        .await?;

        Ok(description)
    }

    /// Retrieves the recursive dependencies of applications and applies a topological sort.
    pub async fn find_dependencies(
        &mut self,
        mut stack: Vec<UserApplicationId>,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<Owner>>,
    ) -> Result<Vec<UserApplicationId>, ExecutionError> {
        // What we return at the end.
        let mut result = Vec::new();
        // The entries already inserted in `result`.
        let mut sorted = HashSet::new();
        // The entries for which dependencies have already been pushed once to the stack.
        let mut seen = HashSet::new();

        while let Some(id) = stack.pop() {
            if sorted.contains(&id) {
                continue;
            }
            if seen.contains(&id) {
                // Second time we see this entry. It was last pushed just before its
                // dependencies -- which are now fully sorted.
                sorted.insert(id);
                result.push(id);
                continue;
            }
            // First time we see this entry:
            // 1. Mark it so that its dependencies are no longer pushed to the stack.
            seen.insert(id);
            // 2. Schedule all the (yet unseen) dependencies, then this entry for a second visit.
            stack.push(id);
            let app = self
                .describe_application(id, Some(txn_tracker), Some(resource_controller))
                .await?;
            for child in app.required_application_ids.iter().rev() {
                if !seen.contains(child) {
                    stack.push(*child);
                }
            }
        }
        Ok(result)
    }

    /// Records a blob that is used in this block. If this is the first use on this chain, creates
    /// an oracle response for it.
    pub(crate) async fn blob_used(
        &mut self,
        maybe_txn_tracker: Option<&mut TransactionTracker>,
        maybe_resource_controller: Option<&mut ResourceController<Option<Owner>>>,
        blob_id: BlobId,
        size: usize,
    ) -> Result<bool, ExecutionError> {
        if let Some(resource_controller) = maybe_resource_controller {
            resource_controller
                .with_state(self)
                .await?
                .track_blob_bytes_read(size as u64)?;
        }
        if self.used_blobs.contains(&blob_id).await? {
            return Ok(false); // Nothing to do.
        }
        self.used_blobs.insert(&blob_id)?;
        if let Some(txn_tracker) = maybe_txn_tracker {
            txn_tracker.replay_oracle_response(OracleResponse::Blob(blob_id))?;
        }
        Ok(true)
    }

    /// Records a blob that is published in this block. This does not create an oracle entry, and
    /// the blob can be used without using an oracle in the future on this chain.
    fn blob_published(&mut self, blob_id: &BlobId) -> Result<(), ExecutionError> {
        self.used_blobs.insert(blob_id)?;
        Ok(())
    }

    pub async fn read_blob_content(&self, blob_id: BlobId) -> Result<BlobContent, ExecutionError> {
        let blobs = self.context().extra().get_blobs(&[blob_id]).await?;
        Ok(blobs.into_iter().next().unwrap().into())
    }

    pub async fn assert_blob_exists(&mut self, blob_id: BlobId) -> Result<(), ExecutionError> {
        if self.context().extra().contains_blob(blob_id).await? {
            Ok(())
        } else {
            Err(ExecutionError::BlobsNotFound(vec![blob_id]))
        }
    }

    async fn check_bytecode_blobs(
        &mut self,
        module_id: &ModuleId,
    ) -> Result<(Blob, Blob), ExecutionError> {
        let contract_bytecode_blob_id =
            BlobId::new(module_id.contract_blob_hash, BlobType::ContractBytecode);
        let service_bytecode_blob_id =
            BlobId::new(module_id.service_blob_hash, BlobType::ServiceBytecode);
        let extra = self.context().extra();
        let mut blobs = extra
            .get_blobs(&[contract_bytecode_blob_id, service_bytecode_blob_id])
            .await?
            .into_iter();
        Ok((blobs.next().unwrap(), blobs.next().unwrap()))
    }
}
