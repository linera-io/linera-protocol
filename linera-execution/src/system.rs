// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
#[path = "./unit_tests/system_tests.rs"]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashSet};

use allocative::Allocative;
use custom_debug_derive::Debug;
use linera_base::{
    crypto::CryptoHash,
    data_types::{
        Amount, ApplicationPermissions, ArithmeticError, Blob, BlobContent, BlockHeight,
        ChainDescription, ChainOrigin, Epoch, InitialChainConfig, OracleResponse, Timestamp,
    },
    ensure, hex_debug,
    identifiers::{Account, AccountOwner, BlobId, BlobType, ChainId, EventId, ModuleId, StreamId},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_views::{
    context::Context,
    map_view::MapView,
    register_view::RegisterView,
    set_view::SetView,
    views::{ClonableView, ReplaceContext, View},
};
use serde::{Deserialize, Serialize};

#[cfg(test)]
use crate::test_utils::SystemExecutionState;
use crate::{
    committee::Committee, util::OracleResponseExt as _, ApplicationDescription, ApplicationId,
    ExecutionError, ExecutionRuntimeContext, MessageContext, MessageKind, OperationContext,
    OutgoingMessage, QueryContext, QueryOutcome, ResourceController, TransactionTracker,
};

/// The event stream name for new epochs and committees.
pub static EPOCH_STREAM_NAME: &[u8] = &[0];
/// The event stream name for removed epochs.
pub static REMOVED_EPOCH_STREAM_NAME: &[u8] = &[1];

/// The data stored in an epoch creation event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochEventData {
    /// The hash of the committee blob for this epoch.
    pub blob_hash: CryptoHash,
    /// The timestamp when the epoch was created on the admin chain.
    pub timestamp: Timestamp,
}

/// The number of times the [`SystemOperation::OpenChain`] was executed.
#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::register_int_counter_vec;
    use prometheus::IntCounterVec;

    pub static OPEN_CHAIN_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "open_chain_count",
            "The number of times the `OpenChain` operation was executed",
            &[],
        )
    });
}

/// A view accessing the execution state of the system of a chain.
#[derive(Debug, ClonableView, View, Allocative)]
#[allocative(bound = "C")]
pub struct SystemExecutionStateView<C> {
    /// How the chain was created. May be unknown for inactive chains.
    pub description: RegisterView<C, Option<ChainDescription>>,
    /// The number identifying the current configuration.
    pub epoch: RegisterView<C, Epoch>,
    /// The admin of the chain.
    pub admin_chain_id: RegisterView<C, Option<ChainId>>,
    /// The committees that we trust, indexed by epoch number.
    // Not using a `MapView` because the set active of committees is supposed to be
    // small. Plus, currently, we would create the `BTreeMap` anyway in various places
    // (e.g. the `OpenChain` operation).
    pub committees: RegisterView<C, BTreeMap<Epoch, Committee>>,
    /// Ownership of the chain.
    pub ownership: RegisterView<C, ChainOwnership>,
    /// Balance of the chain. (Available to any user able to create blocks in the chain.)
    pub balance: RegisterView<C, Amount>,
    /// Balances attributed to a given owner.
    pub balances: MapView<C, AccountOwner, Amount>,
    /// The timestamp of the most recent block.
    pub timestamp: RegisterView<C, Timestamp>,
    /// Whether this chain has been closed.
    pub closed: RegisterView<C, bool>,
    /// Permissions for applications on this chain.
    pub application_permissions: RegisterView<C, ApplicationPermissions>,
    /// Blobs that have been used or published on this chain.
    pub used_blobs: SetView<C, BlobId>,
    /// The event stream subscriptions of applications on this chain.
    pub event_subscriptions: MapView<C, (ChainId, StreamId), EventSubscriptions>,
    /// The number of events in the streams that this chain is writing to.
    pub stream_event_counts: MapView<C, StreamId, u32>,
}

impl<C: Context, C2: Context> ReplaceContext<C2> for SystemExecutionStateView<C> {
    type Target = SystemExecutionStateView<C2>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        SystemExecutionStateView {
            description: self.description.with_context(ctx.clone()).await,
            epoch: self.epoch.with_context(ctx.clone()).await,
            admin_chain_id: self.admin_chain_id.with_context(ctx.clone()).await,
            committees: self.committees.with_context(ctx.clone()).await,
            ownership: self.ownership.with_context(ctx.clone()).await,
            balance: self.balance.with_context(ctx.clone()).await,
            balances: self.balances.with_context(ctx.clone()).await,
            timestamp: self.timestamp.with_context(ctx.clone()).await,
            closed: self.closed.with_context(ctx.clone()).await,
            application_permissions: self.application_permissions.with_context(ctx.clone()).await,
            used_blobs: self.used_blobs.with_context(ctx.clone()).await,
            event_subscriptions: self.event_subscriptions.with_context(ctx.clone()).await,
            stream_event_counts: self.stream_event_counts.with_context(ctx.clone()).await,
        }
    }
}

/// The applications subscribing to a particular stream, and the next event index.
#[derive(Debug, Default, Clone, Serialize, Deserialize, Allocative)]
pub struct EventSubscriptions {
    /// The next event index, i.e. the total number of events in this stream that have already
    /// been processed by this chain.
    pub next_index: u32,
    /// The applications that are subscribed to this stream.
    pub applications: BTreeSet<ApplicationId>,
}

/// The initial configuration for a new chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Allocative)]
pub struct OpenChainConfig {
    /// The ownership configuration of the new chain.
    pub ownership: ChainOwnership,
    /// The initial chain balance.
    pub balance: Amount,
    /// The initial application permissions.
    pub application_permissions: ApplicationPermissions,
}

impl OpenChainConfig {
    /// Creates an [`InitialChainConfig`] based on this [`OpenChainConfig`] and additional
    /// parameters.
    pub fn init_chain_config(
        &self,
        epoch: Epoch,
        min_active_epoch: Epoch,
        max_active_epoch: Epoch,
    ) -> InitialChainConfig {
        InitialChainConfig {
            application_permissions: self.application_permissions.clone(),
            balance: self.balance,
            epoch,
            min_active_epoch,
            max_active_epoch,
            ownership: self.ownership.clone(),
        }
    }
}

/// A system operation.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Allocative)]
pub enum SystemOperation {
    /// Transfers `amount` units of value from the given owner's account to the recipient.
    /// If no owner is given, try to take the units out of the unattributed account.
    Transfer {
        owner: AccountOwner,
        recipient: Account,
        amount: Amount,
    },
    /// Claims `amount` units of value from the given owner's account in the remote
    /// `target` chain. Depending on its configuration, the `target` chain may refuse to
    /// process the message.
    Claim {
        owner: AccountOwner,
        target_id: ChainId,
        recipient: Account,
        amount: Amount,
    },
    /// Creates (or activates) a new chain.
    /// This will automatically subscribe to the future committees created by `admin_chain_id`.
    OpenChain(OpenChainConfig),
    /// Closes the chain.
    CloseChain,
    /// Changes the ownership of the chain.
    ChangeOwnership {
        /// Super owners can propose fast blocks in the first round, and regular blocks in any round.
        #[debug(skip_if = Vec::is_empty)]
        super_owners: Vec<AccountOwner>,
        /// The regular owners, with their weights that determine how often they are round leader.
        #[debug(skip_if = Vec::is_empty)]
        owners: Vec<(AccountOwner, u64)>,
        /// The leader of the first single-leader round. If not set, this is random like other rounds.
        #[debug(skip_if = Option::is_none)]
        first_leader: Option<AccountOwner>,
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
    /// Publishes a new application module.
    PublishModule { module_id: ModuleId },
    /// Publishes a new data blob.
    PublishDataBlob { blob_hash: CryptoHash },
    /// Verifies that the given blob exists. Otherwise the block fails.
    VerifyBlob { blob_id: BlobId },
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
        required_application_ids: Vec<ApplicationId>,
    },
    /// Operations that are only allowed on the admin chain.
    Admin(AdminOperation),
    /// Processes an event about a new epoch and committee.
    ProcessNewEpoch(Epoch),
    /// Processes an event about a removed epoch and committee.
    ProcessRemovedEpoch(Epoch),
    /// Updates the event stream trackers.
    UpdateStreams(Vec<(ChainId, StreamId, u32)>),
}

/// Operations that are only allowed on the admin chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Allocative)]
pub enum AdminOperation {
    /// Publishes a new committee as a blob. This can be assigned to an epoch using
    /// [`AdminOperation::CreateCommittee`] in a later block.
    PublishCommitteeBlob { blob_hash: CryptoHash },
    /// Registers a new committee. Other chains can then migrate to the new epoch by executing
    /// [`SystemOperation::ProcessNewEpoch`].
    CreateCommittee { epoch: Epoch, blob_hash: CryptoHash },
    /// Removes a committee. Other chains should execute [`SystemOperation::ProcessRemovedEpoch`],
    /// so that blocks from the retired epoch will not be accepted until they are followed (hence
    /// re-certified) by a block certified by a recent committee.
    RemoveCommittee { epoch: Epoch },
}

/// A system message meant to be executed on a remote chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Allocative)]
pub enum SystemMessage {
    /// Credits `amount` units of value to the account `target` -- unless the message is
    /// bouncing, in which case `source` is credited instead.
    Credit {
        target: AccountOwner,
        amount: Amount,
        source: AccountOwner,
    },
    /// Withdraws `amount` units of value from the account and starts a transfer to credit
    /// the recipient. The message must be properly authenticated. Receiver chains may
    /// refuse it depending on their configuration.
    Withdraw {
        owner: AccountOwner,
        amount: Amount,
        recipient: Account,
    },
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
    pub app_id: ApplicationId,
}

impl<C> SystemExecutionStateView<C>
where
    C: Context + Clone + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    /// Invariant for the states of active chains.
    pub fn is_active(&self) -> bool {
        self.description.get().is_some()
            && self.ownership.get().is_active()
            && self.current_committee().is_some()
            && self.admin_chain_id.get().is_some()
    }

    /// Returns the current committee, if any.
    pub fn current_committee(&self) -> Option<(Epoch, &Committee)> {
        let epoch = self.epoch.get();
        let committee = self.committees.get().get(epoch)?;
        Some((*epoch, committee))
    }

    async fn get_event(&self, event_id: EventId) -> Result<Vec<u8>, ExecutionError> {
        match self.context().extra().get_event(event_id.clone()).await? {
            None => Err(ExecutionError::EventsNotFound(vec![event_id])),
            Some(vec) => Ok(vec),
        }
    }

    /// Executes the sender's side of an operation and returns a list of actions to be
    /// taken.
    pub async fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: SystemOperation,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<AccountOwner>>,
    ) -> Result<Option<(ApplicationId, Vec<u8>)>, ExecutionError> {
        use SystemOperation::*;
        let mut new_application = None;
        match operation {
            OpenChain(config) => {
                let _chain_id = self
                    .open_chain(
                        config,
                        context.chain_id,
                        context.height,
                        context.timestamp,
                        txn_tracker,
                    )
                    .await?;
                #[cfg(with_metrics)]
                metrics::OPEN_CHAIN_COUNT.with_label_values(&[]).inc();
            }
            ChangeOwnership {
                super_owners,
                owners,
                first_leader,
                multi_leader_rounds,
                open_multi_leader_rounds,
                timeout_config,
            } => {
                self.ownership.set(ChainOwnership {
                    super_owners: super_owners.into_iter().collect(),
                    owners: owners.into_iter().collect(),
                    first_leader,
                    multi_leader_rounds,
                    open_multi_leader_rounds,
                    timeout_config,
                });
            }
            ChangeApplicationPermissions(application_permissions) => {
                self.application_permissions.set(application_permissions);
            }
            CloseChain => self.close_chain(),
            Transfer {
                owner,
                amount,
                recipient,
            } => {
                let maybe_message = self
                    .transfer(context.authenticated_owner, None, owner, recipient, amount)
                    .await?;
                txn_tracker.add_outgoing_messages(maybe_message);
            }
            Claim {
                owner,
                target_id,
                recipient,
                amount,
            } => {
                let maybe_message = self
                    .claim(
                        context.authenticated_owner,
                        None,
                        owner,
                        target_id,
                        recipient,
                        amount,
                    )
                    .await?;
                txn_tracker.add_outgoing_messages(maybe_message);
            }
            Admin(admin_operation) => {
                ensure!(
                    *self.admin_chain_id.get() == Some(context.chain_id),
                    ExecutionError::AdminOperationOnNonAdminChain
                );
                match admin_operation {
                    AdminOperation::PublishCommitteeBlob { blob_hash } => {
                        self.blob_published(
                            &BlobId::new(blob_hash, BlobType::Committee),
                            txn_tracker,
                        )?;
                    }
                    AdminOperation::CreateCommittee { epoch, blob_hash } => {
                        self.check_next_epoch(epoch)?;
                        let blob_id = BlobId::new(blob_hash, BlobType::Committee);
                        let committee =
                            bcs::from_bytes(self.read_blob_content(blob_id).await?.bytes())?;
                        self.blob_used(txn_tracker, blob_id).await?;
                        self.committees.get_mut().insert(epoch, committee);
                        self.epoch.set(epoch);
                        let event_data = EpochEventData {
                            blob_hash,
                            timestamp: context.timestamp,
                        };
                        txn_tracker.add_event(
                            StreamId::system(EPOCH_STREAM_NAME),
                            epoch.0,
                            bcs::to_bytes(&event_data)?,
                        );
                    }
                    AdminOperation::RemoveCommittee { epoch } => {
                        ensure!(
                            self.committees.get_mut().remove(&epoch).is_some(),
                            ExecutionError::InvalidCommitteeRemoval
                        );
                        txn_tracker.add_event(
                            StreamId::system(REMOVED_EPOCH_STREAM_NAME),
                            epoch.0,
                            vec![],
                        );
                    }
                }
            }
            PublishModule { module_id } => {
                for blob_id in module_id.bytecode_blob_ids() {
                    self.blob_published(&blob_id, txn_tracker)?;
                }
            }
            CreateApplication {
                module_id,
                parameters,
                instantiation_argument,
                required_application_ids,
            } => {
                let CreateApplicationResult { app_id } = self
                    .create_application(
                        context.chain_id,
                        context.height,
                        module_id,
                        parameters,
                        required_application_ids,
                        txn_tracker,
                    )
                    .await?;
                new_application = Some((app_id, instantiation_argument));
            }
            PublishDataBlob { blob_hash } => {
                self.blob_published(&BlobId::new(blob_hash, BlobType::Data), txn_tracker)?;
            }
            VerifyBlob { blob_id } => {
                self.assert_blob_exists(blob_id).await?;
                resource_controller
                    .with_state(self)
                    .await?
                    .track_blob_read(0)?;
                self.blob_used(txn_tracker, blob_id).await?;
            }
            ProcessNewEpoch(epoch) => {
                self.check_next_epoch(epoch)?;
                let admin_chain_id = self
                    .admin_chain_id
                    .get()
                    .ok_or_else(|| ExecutionError::InactiveChain(context.chain_id))?;
                let event_id = EventId {
                    chain_id: admin_chain_id,
                    stream_id: StreamId::system(EPOCH_STREAM_NAME),
                    index: epoch.0,
                };
                let bytes = txn_tracker
                    .oracle(|| async {
                        let bytes = self.get_event(event_id.clone()).await?;
                        Ok(OracleResponse::Event(event_id.clone(), bytes))
                    })
                    .await?
                    .to_event(&event_id)?;
                let event_data: EpochEventData = bcs::from_bytes(&bytes)?;
                let blob_id = BlobId::new(event_data.blob_hash, BlobType::Committee);
                let committee = bcs::from_bytes(self.read_blob_content(blob_id).await?.bytes())?;
                self.blob_used(txn_tracker, blob_id).await?;
                self.committees.get_mut().insert(epoch, committee);
                self.epoch.set(epoch);
            }
            ProcessRemovedEpoch(epoch) => {
                ensure!(
                    self.committees.get_mut().remove(&epoch).is_some(),
                    ExecutionError::InvalidCommitteeRemoval
                );
                let admin_chain_id = self
                    .admin_chain_id
                    .get()
                    .ok_or_else(|| ExecutionError::InactiveChain(context.chain_id))?;
                let event_id = EventId {
                    chain_id: admin_chain_id,
                    stream_id: StreamId::system(REMOVED_EPOCH_STREAM_NAME),
                    index: epoch.0,
                };
                txn_tracker
                    .oracle(|| async {
                        let bytes = self.get_event(event_id.clone()).await?;
                        Ok(OracleResponse::Event(event_id, bytes))
                    })
                    .await?;
            }
            UpdateStreams(streams) => {
                let mut missing_events = Vec::new();
                for (chain_id, stream_id, next_index) in streams {
                    let subscriptions = self
                        .event_subscriptions
                        .get_mut_or_default(&(chain_id, stream_id.clone()))
                        .await?;
                    ensure!(
                        subscriptions.next_index < next_index,
                        ExecutionError::OutdatedUpdateStreams
                    );
                    for application_id in &subscriptions.applications {
                        txn_tracker.add_stream_to_process(
                            *application_id,
                            chain_id,
                            stream_id.clone(),
                            subscriptions.next_index,
                            next_index,
                        );
                    }
                    subscriptions.next_index = next_index;
                    let index = next_index
                        .checked_sub(1)
                        .ok_or(ArithmeticError::Underflow)?;
                    let event_id = EventId {
                        chain_id,
                        stream_id,
                        index,
                    };
                    let context = self.context();
                    let extra = context.extra();
                    txn_tracker
                        .oracle(|| async {
                            if !extra.contains_event(event_id.clone()).await? {
                                missing_events.push(event_id.clone());
                            }
                            Ok(OracleResponse::EventExists(event_id))
                        })
                        .await?;
                }
                ensure!(
                    missing_events.is_empty(),
                    ExecutionError::EventsNotFound(missing_events)
                );
            }
        }

        Ok(new_application)
    }

    /// Returns an error if the `provided` epoch is not exactly one higher than the chain's current
    /// epoch.
    fn check_next_epoch(&self, provided: Epoch) -> Result<(), ExecutionError> {
        let expected = self.epoch.get().try_add_one()?;
        ensure!(
            provided == expected,
            ExecutionError::InvalidCommitteeEpoch { provided, expected }
        );
        Ok(())
    }

    async fn credit(&mut self, owner: &AccountOwner, amount: Amount) -> Result<(), ExecutionError> {
        if owner == &AccountOwner::CHAIN {
            let new_balance = self.balance.get().saturating_add(amount);
            self.balance.set(new_balance);
        } else {
            let balance = self.balances.get_mut_or_default(owner).await?;
            *balance = balance.saturating_add(amount);
        }
        Ok(())
    }

    async fn credit_or_send_message(
        &mut self,
        source: AccountOwner,
        recipient: Account,
        amount: Amount,
    ) -> Result<Option<OutgoingMessage>, ExecutionError> {
        let source_chain_id = self.context().extra().chain_id();
        if recipient.chain_id == source_chain_id {
            // Handle same-chain transfer locally.
            let target = recipient.owner;
            self.credit(&target, amount).await?;
            Ok(None)
        } else {
            // Handle cross-chain transfer with message.
            let message = SystemMessage::Credit {
                amount,
                source,
                target: recipient.owner,
            };
            Ok(Some(
                OutgoingMessage::new(recipient.chain_id, message).with_kind(MessageKind::Tracked),
            ))
        }
    }

    pub async fn transfer(
        &mut self,
        authenticated_owner: Option<AccountOwner>,
        authenticated_application_id: Option<ApplicationId>,
        source: AccountOwner,
        recipient: Account,
        amount: Amount,
    ) -> Result<Option<OutgoingMessage>, ExecutionError> {
        if source == AccountOwner::CHAIN {
            ensure!(
                authenticated_owner.is_some()
                    && self.ownership.get().is_owner(&authenticated_owner.unwrap()),
                ExecutionError::UnauthenticatedTransferOwner
            );
        } else {
            ensure!(
                authenticated_owner == Some(source)
                    || authenticated_application_id.map(AccountOwner::from) == Some(source),
                ExecutionError::UnauthenticatedTransferOwner
            );
        }
        ensure!(
            amount > Amount::ZERO,
            ExecutionError::IncorrectTransferAmount
        );
        self.debit(&source, amount).await?;
        self.credit_or_send_message(source, recipient, amount).await
    }

    pub async fn claim(
        &mut self,
        authenticated_owner: Option<AccountOwner>,
        authenticated_application_id: Option<ApplicationId>,
        source: AccountOwner,
        target_id: ChainId,
        recipient: Account,
        amount: Amount,
    ) -> Result<Option<OutgoingMessage>, ExecutionError> {
        ensure!(
            authenticated_owner == Some(source)
                || authenticated_application_id.map(AccountOwner::from) == Some(source),
            ExecutionError::UnauthenticatedClaimOwner
        );
        ensure!(amount > Amount::ZERO, ExecutionError::IncorrectClaimAmount);

        let current_chain_id = self.context().extra().chain_id();
        if target_id == current_chain_id {
            // Handle same-chain claim locally by processing the withdraw operation directly
            self.debit(&source, amount).await?;
            self.credit_or_send_message(source, recipient, amount).await
        } else {
            // Handle cross-chain claim with Withdraw message
            let message = SystemMessage::Withdraw {
                amount,
                owner: source,
                recipient,
            };
            Ok(Some(
                OutgoingMessage::new(target_id, message)
                    .with_authenticated_owner(authenticated_owner),
            ))
        }
    }

    /// Debits an [`Amount`] of tokens from an account's balance.
    async fn debit(
        &mut self,
        account: &AccountOwner,
        amount: Amount,
    ) -> Result<(), ExecutionError> {
        let balance = if account == &AccountOwner::CHAIN {
            self.balance.get_mut()
        } else {
            self.balances.get_mut(account).await?.ok_or_else(|| {
                ExecutionError::InsufficientBalance {
                    balance: Amount::ZERO,
                    account: *account,
                }
            })?
        };

        balance
            .try_sub_assign(amount)
            .map_err(|_| ExecutionError::InsufficientBalance {
                balance: *balance,
                account: *account,
            })?;

        if account != &AccountOwner::CHAIN && balance.is_zero() {
            self.balances.remove(account)?;
        }

        Ok(())
    }

    /// Executes a cross-chain message that represents the recipient's side of an operation.
    pub async fn execute_message(
        &mut self,
        context: MessageContext,
        message: SystemMessage,
    ) -> Result<Vec<OutgoingMessage>, ExecutionError> {
        let mut outcome = Vec::new();
        use SystemMessage::*;
        match message {
            Credit {
                amount,
                source,
                target,
            } => {
                let receiver = if context.is_bouncing { source } else { target };
                self.credit(&receiver, amount).await?;
            }
            Withdraw {
                amount,
                owner,
                recipient,
            } => {
                self.debit(&owner, amount).await?;
                if let Some(message) = self
                    .credit_or_send_message(owner, recipient, amount)
                    .await?
                {
                    outcome.push(message);
                }
            }
        }
        Ok(outcome)
    }

    /// Initializes the system application state on a newly opened chain.
    /// Returns `Ok(true)` if the chain was already initialized, `Ok(false)` if it wasn't.
    pub async fn initialize_chain(&mut self, chain_id: ChainId) -> Result<bool, ExecutionError> {
        if self.description.get().is_some() {
            // already initialized
            return Ok(true);
        }
        let description_blob = self
            .read_blob_content(BlobId::new(chain_id.0, BlobType::ChainDescription))
            .await?;
        let description: ChainDescription = bcs::from_bytes(description_blob.bytes())?;
        let InitialChainConfig {
            ownership,
            epoch,
            balance,
            min_active_epoch,
            max_active_epoch,
            application_permissions,
        } = description.config().clone();
        self.timestamp.set(description.timestamp());
        self.description.set(Some(description));
        self.epoch.set(epoch);

        let committees = self
            .context()
            .extra()
            .get_committees(min_active_epoch..=max_active_epoch)
            .await?;
        let admin_chain_id = self
            .context()
            .extra()
            .get_network_description()
            .await?
            .ok_or(ExecutionError::NoNetworkDescriptionFound)?
            .admin_chain_id;

        self.committees.set(committees);
        self.admin_chain_id.set(Some(admin_chain_id));
        self.ownership.set(ownership);
        self.balance.set(balance);
        self.application_permissions.set(application_permissions);
        Ok(false)
    }

    pub fn handle_query(
        &mut self,
        context: QueryContext,
        _query: SystemQuery,
    ) -> QueryOutcome<SystemResponse> {
        let response = SystemResponse {
            chain_id: context.chain_id,
            balance: *self.balance.get(),
        };
        QueryOutcome {
            response,
            operations: vec![],
        }
    }

    /// Returns the messages to open a new chain, and subtracts the new chain's balance
    /// from this chain's.
    pub async fn open_chain(
        &mut self,
        config: OpenChainConfig,
        parent: ChainId,
        block_height: BlockHeight,
        timestamp: Timestamp,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<ChainId, ExecutionError> {
        let chain_index = txn_tracker.next_chain_index();
        let chain_origin = ChainOrigin::Child {
            parent,
            block_height,
            chain_index,
        };
        let init_chain_config = config.init_chain_config(
            *self.epoch.get(),
            self.committees
                .get()
                .keys()
                .min()
                .copied()
                .unwrap_or(Epoch::ZERO),
            self.committees
                .get()
                .keys()
                .max()
                .copied()
                .unwrap_or(Epoch::ZERO),
        );
        let chain_description = ChainDescription::new(chain_origin, init_chain_config, timestamp);
        let child_id = chain_description.id();
        self.debit(&AccountOwner::CHAIN, config.balance).await?;
        let blob = Blob::new_chain_description(&chain_description);
        txn_tracker.add_created_blob(blob);
        Ok(child_id)
    }

    pub fn close_chain(&mut self) {
        self.closed.set(true);
    }

    pub async fn create_application(
        &mut self,
        chain_id: ChainId,
        block_height: BlockHeight,
        module_id: ModuleId,
        parameters: Vec<u8>,
        required_application_ids: Vec<ApplicationId>,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<CreateApplicationResult, ExecutionError> {
        let application_index = txn_tracker.next_application_index();

        let blob_ids = self.check_bytecode_blobs(&module_id, txn_tracker).await?;
        // We only remember to register the blobs that aren't recorded in `used_blobs`
        // already.
        for blob_id in blob_ids {
            self.blob_used(txn_tracker, blob_id).await?;
        }

        let application_description = ApplicationDescription {
            module_id,
            creator_chain_id: chain_id,
            block_height,
            application_index,
            parameters,
            required_application_ids,
        };
        self.check_required_applications(&application_description, txn_tracker)
            .await?;

        let blob = Blob::new_application_description(&application_description);
        self.used_blobs.insert(&blob.id())?;
        txn_tracker.add_created_blob(blob);

        Ok(CreateApplicationResult {
            app_id: ApplicationId::from(&application_description),
        })
    }

    async fn check_required_applications(
        &mut self,
        application_description: &ApplicationDescription,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<(), ExecutionError> {
        // Make sure that referenced applications IDs have been registered.
        for required_id in &application_description.required_application_ids {
            Box::pin(self.describe_application(*required_id, txn_tracker)).await?;
        }
        Ok(())
    }

    /// Retrieves an application's description.
    pub async fn describe_application(
        &mut self,
        id: ApplicationId,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<ApplicationDescription, ExecutionError> {
        let blob_id = id.description_blob_id();
        let content = match txn_tracker.created_blobs().get(&blob_id) {
            Some(content) => content.clone(),
            None => self.read_blob_content(blob_id).await?,
        };
        self.blob_used(txn_tracker, blob_id).await?;
        let description: ApplicationDescription = bcs::from_bytes(content.bytes())?;

        let blob_ids = self
            .check_bytecode_blobs(&description.module_id, txn_tracker)
            .await?;
        // We only remember to register the blobs that aren't recorded in `used_blobs`
        // already.
        for blob_id in blob_ids {
            self.blob_used(txn_tracker, blob_id).await?;
        }

        self.check_required_applications(&description, txn_tracker)
            .await?;

        Ok(description)
    }

    /// Retrieves the recursive dependencies of applications and applies a topological sort.
    pub async fn find_dependencies(
        &mut self,
        mut stack: Vec<ApplicationId>,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<Vec<ApplicationId>, ExecutionError> {
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
            let app = self.describe_application(id, txn_tracker).await?;
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
        txn_tracker: &mut TransactionTracker,
        blob_id: BlobId,
    ) -> Result<bool, ExecutionError> {
        if self.used_blobs.contains(&blob_id).await? {
            return Ok(false); // Nothing to do.
        }
        self.used_blobs.insert(&blob_id)?;
        txn_tracker.replay_oracle_response(OracleResponse::Blob(blob_id))?;
        Ok(true)
    }

    /// Records a blob that is published in this block. This does not create an oracle entry, and
    /// the blob can be used without using an oracle in the future on this chain.
    fn blob_published(
        &mut self,
        blob_id: &BlobId,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<(), ExecutionError> {
        self.used_blobs.insert(blob_id)?;
        txn_tracker.add_published_blob(*blob_id);
        Ok(())
    }

    pub async fn read_blob_content(&self, blob_id: BlobId) -> Result<BlobContent, ExecutionError> {
        match self.context().extra().get_blob(blob_id).await {
            Ok(Some(blob)) => Ok(blob.into()),
            Ok(None) => Err(ExecutionError::BlobsNotFound(vec![blob_id])),
            Err(error) => Err(error.into()),
        }
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
        txn_tracker: &TransactionTracker,
    ) -> Result<Vec<BlobId>, ExecutionError> {
        let blob_ids = module_id.bytecode_blob_ids();

        let mut missing_blobs = Vec::new();
        for blob_id in &blob_ids {
            // First check if blob is present in created_blobs
            if txn_tracker.created_blobs().contains_key(blob_id) {
                continue; // Blob found in created_blobs, it's ok
            }
            // If not in created_blobs, check storage
            if !self.context().extra().contains_blob(*blob_id).await? {
                missing_blobs.push(*blob_id);
            }
        }
        ensure!(
            missing_blobs.is_empty(),
            ExecutionError::BlobsNotFound(missing_blobs)
        );

        Ok(blob_ids)
    }
}
