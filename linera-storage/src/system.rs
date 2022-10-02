// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    committee::Committee,
    ensure,
    error::Error,
    manager::ChainManager,
    messages::{ChainDescription, ChainId, ChannelId, Destination, Effect, EffectId, Epoch},
    system::{Address, Amount, Balance, SystemEffect, SystemOperation, ADMIN_CHANNEL},
};
use linera_execution::{EffectContext, OperationContext, RawApplicationResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The execution state of the system of a chain.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct SystemExecutionState {
    /// How the chain was created. May be unknown for inactive chains.
    pub description: Option<ChainDescription>,
    /// The number identifying the current configuration.
    pub epoch: Option<Epoch>,
    /// The admin of the chain.
    pub admin_id: Option<ChainId>,
    /// Track the channels that we have subscribed to.
    /// We avoid BTreeSet<String> because of a Serde/BCS limitation.
    pub subscriptions: BTreeMap<ChannelId, ()>,
    /// The committees that we trust, indexed by epoch number.
    pub committees: BTreeMap<Epoch, Committee>,
    /// Manager of the chain.
    pub manager: ChainManager,
    /// Balance of the chain.
    pub balance: Balance,
}

impl SystemExecutionState {
    /// Invariant for the states of active chains.
    pub fn is_active(&self) -> bool {
        self.description.is_some()
            && self.manager.is_active()
            && self.epoch.is_some()
            && self.committees.contains_key(self.epoch.as_ref().unwrap())
            && self.admin_id.is_some()
    }
}

impl SystemExecutionState {
    /// Execute the sender's side of the operation.
    /// Return a list of recipients who need to be notified.
    pub fn apply_operation(
        &mut self,
        context: &OperationContext,
        operation: &SystemOperation,
    ) -> Result<RawApplicationResult<SystemEffect>, Error> {
        use SystemOperation::*;
        match operation {
            OpenChain {
                id,
                owner,
                committees,
                admin_id,
                epoch,
            } => {
                let expected_id = ChainId::child(context.clone().into());
                ensure!(id == &expected_id, Error::InvalidNewChainId(*id));
                ensure!(
                    self.admin_id.as_ref() == Some(admin_id),
                    Error::InvalidNewChainAdminId(*id)
                );
                ensure!(&self.committees == committees, Error::InvalidCommittees);
                ensure!(
                    self.epoch.as_ref() == Some(epoch),
                    Error::InvalidEpoch {
                        chain_id: *id,
                        epoch: *epoch
                    }
                );
                let e1 = (
                    Destination::Recipient(*id),
                    SystemEffect::OpenChain {
                        id: *id,
                        owner: *owner,
                        committees: committees.clone(),
                        admin_id: *admin_id,
                        epoch: *epoch,
                    },
                );
                let e2 = (
                    Destination::Recipient(*admin_id),
                    SystemEffect::Subscribe {
                        id: *id,
                        channel: ChannelId {
                            chain_id: *admin_id,
                            name: ADMIN_CHANNEL.into(),
                        },
                    },
                );
                let application = RawApplicationResult {
                    effects: vec![e1, e2],
                    subscribe: None,
                    unsubscribe: None,
                };
                Ok(application)
            }
            ChangeOwner { new_owner } => {
                self.manager = ChainManager::single(*new_owner);
                Ok(RawApplicationResult::default())
            }
            ChangeMultipleOwners { new_owners } => {
                self.manager = ChainManager::multiple(new_owners.clone());
                Ok(RawApplicationResult::default())
            }
            CloseChain => {
                self.manager = ChainManager::default();
                // Unsubscribe to all channels.
                let subscriptions = std::mem::take(&mut self.subscriptions);
                let mut effects = Vec::new();
                for (channel, ()) in subscriptions {
                    effects.push((
                        Destination::Recipient(channel.chain_id),
                        SystemEffect::Unsubscribe {
                            id: context.chain_id,
                            channel,
                        },
                    ));
                }
                let application = RawApplicationResult {
                    effects,
                    subscribe: None,
                    unsubscribe: None,
                };
                Ok(application)
            }
            Transfer {
                amount, recipient, ..
            } => {
                ensure!(*amount > Amount::zero(), Error::IncorrectTransferAmount);
                ensure!(
                    self.balance >= (*amount).into(),
                    Error::InsufficientFunding {
                        current_balance: self.balance
                    }
                );
                self.balance.try_sub_assign((*amount).into())?;
                let application = match recipient {
                    Address::Burn => RawApplicationResult::default(),
                    Address::Account(id) => RawApplicationResult {
                        effects: vec![(
                            Destination::Recipient(*id),
                            SystemEffect::Credit {
                                amount: *amount,
                                recipient: *id,
                            },
                        )],
                        subscribe: None,
                        unsubscribe: None,
                    },
                };
                Ok(application)
            }
            CreateCommittee {
                admin_id,
                epoch,
                committee,
            } => {
                // We are the admin chain and want to create a committee.
                ensure!(
                    *admin_id == context.chain_id,
                    Error::InvalidCommitteeCreation
                );
                ensure!(
                    Some(admin_id) == self.admin_id.as_ref(),
                    Error::InvalidCommitteeCreation
                );
                ensure!(
                    *epoch == self.epoch.expect("chain is active").try_add_one()?,
                    Error::InvalidCommitteeCreation
                );
                self.committees.insert(*epoch, committee.clone());
                self.epoch = Some(*epoch);
                let application = RawApplicationResult {
                    effects: vec![(
                        Destination::Subscribers(ADMIN_CHANNEL.into()),
                        SystemEffect::SetCommittees {
                            admin_id: *admin_id,
                            epoch: self.epoch.expect("chain is active"),
                            committees: self.committees.clone(),
                        },
                    )],
                    subscribe: None,
                    unsubscribe: None,
                };
                Ok(application)
            }
            RemoveCommittee { admin_id, epoch } => {
                // We are the admin chain and want to remove a committee.
                ensure!(
                    *admin_id == context.chain_id,
                    Error::InvalidCommitteeRemoval
                );
                ensure!(
                    Some(admin_id) == self.admin_id.as_ref(),
                    Error::InvalidCommitteeRemoval
                );
                ensure!(
                    self.committees.remove(epoch).is_some(),
                    Error::InvalidCommitteeRemoval
                );
                let application = RawApplicationResult {
                    effects: vec![(
                        Destination::Subscribers(ADMIN_CHANNEL.into()),
                        SystemEffect::SetCommittees {
                            admin_id: *admin_id,
                            epoch: self.epoch.expect("chain is active"),
                            committees: self.committees.clone(),
                        },
                    )],
                    subscribe: None,
                    unsubscribe: None,
                };
                Ok(application)
            }
            SubscribeToNewCommittees { admin_id } => {
                // We should not subscribe to ourself in this case.
                ensure!(
                    context.chain_id != *admin_id,
                    Error::InvalidSubscriptionToNewCommittees(context.chain_id)
                );
                ensure!(
                    self.admin_id.as_ref() == Some(admin_id),
                    Error::InvalidSubscriptionToNewCommittees(context.chain_id)
                );
                let channel_id = ChannelId {
                    chain_id: *admin_id,
                    name: ADMIN_CHANNEL.into(),
                };
                ensure!(
                    !self.subscriptions.contains_key(&channel_id),
                    Error::InvalidSubscriptionToNewCommittees(context.chain_id)
                );
                self.subscriptions.insert(channel_id, ());
                let application = RawApplicationResult {
                    effects: vec![(
                        Destination::Recipient(*admin_id),
                        SystemEffect::Subscribe {
                            id: context.chain_id,
                            channel: ChannelId {
                                chain_id: *admin_id,
                                name: ADMIN_CHANNEL.into(),
                            },
                        },
                    )],
                    subscribe: None,
                    unsubscribe: None,
                };
                Ok(application)
            }
            UnsubscribeToNewCommittees { admin_id } => {
                let channel_id = ChannelId {
                    chain_id: *admin_id,
                    name: ADMIN_CHANNEL.into(),
                };
                ensure!(
                    self.subscriptions.contains_key(&channel_id),
                    Error::InvalidUnsubscriptionToNewCommittees(context.chain_id)
                );
                self.subscriptions.remove(&channel_id);
                let application = RawApplicationResult {
                    effects: vec![(
                        Destination::Recipient(*admin_id),
                        SystemEffect::Unsubscribe {
                            id: context.chain_id,
                            channel: ChannelId {
                                chain_id: *admin_id,
                                name: ADMIN_CHANNEL.into(),
                            },
                        },
                    )],
                    subscribe: None,
                    unsubscribe: None,
                };
                Ok(application)
            }
        }
    }

    /// Execute the recipient's side of an operation, aka a "remote effect".
    /// Effects must be executed by order of heights in the sender's chain.
    pub fn apply_effect(
        &mut self,
        context: &EffectContext,
        effect: &SystemEffect,
    ) -> Result<RawApplicationResult<SystemEffect>, Error> {
        use SystemEffect::*;
        match effect {
            Credit { amount, recipient } if context.chain_id == *recipient => {
                self.balance = self
                    .balance
                    .try_add((*amount).into())
                    .unwrap_or_else(|_| Balance::max());
                Ok(RawApplicationResult::default())
            }
            SetCommittees {
                admin_id,
                epoch,
                committees,
            } if self.admin_id.as_ref() == Some(admin_id) => {
                // This chain was not yet subscribed at the time earlier epochs were broadcast.
                ensure!(
                    *epoch >= self.epoch.expect("chain is active"),
                    Error::InvalidCrossChainRequest
                );
                self.epoch = Some(*epoch);
                self.committees = committees.clone();
                Ok(RawApplicationResult::default())
            }
            Subscribe { id, channel } if channel.chain_id == context.chain_id => {
                // Notify the subscriber about this block, so that it is included in the
                // receive_log of the subscriber and correctly synchronized.
                let application = RawApplicationResult {
                    effects: vec![(
                        Destination::Recipient(*id),
                        SystemEffect::Notify { id: *id },
                    )],
                    subscribe: Some((channel.name.clone(), *id)),
                    unsubscribe: None,
                };
                Ok(application)
            }
            Unsubscribe { id, channel } if channel.chain_id == context.chain_id => {
                let application = RawApplicationResult {
                    effects: vec![(
                        Destination::Recipient(*id),
                        SystemEffect::Notify { id: *id },
                    )],
                    subscribe: None,
                    unsubscribe: Some((channel.name.clone(), *id)),
                };
                Ok(application)
            }
            Notify { .. } => Ok(RawApplicationResult::default()),
            OpenChain { .. } => {
                // This special effect is executed immediately when cross-chain requests are received.
                Ok(RawApplicationResult::default())
            }
            _ => {
                log::error!("Skipping unexpected received effect: {effect:?}");
                Ok(RawApplicationResult::default())
            }
        }
    }

    /// Execute certain effects immediately upon receiving a message.
    pub fn apply_immediate_effect(
        &mut self,
        this_chain_id: ChainId,
        effect_id: EffectId,
        effect: &Effect,
    ) -> Result<bool, Error> {
        // Chain creation effects are special and executed (only) in this callback.
        // For simplicity, they will still appear in the received messages.
        match &effect {
            Effect::System(SystemEffect::OpenChain {
                id,
                owner,
                epoch,
                committees,
                admin_id,
            }) if id == &this_chain_id => {
                // Guaranteed under BFT assumptions.
                assert!(self.description.is_none());
                assert!(!self.manager.is_active());
                assert!(self.committees.is_empty());
                let description = ChainDescription::Child(effect_id);
                assert_eq!(this_chain_id, description.into());
                self.description = Some(description);
                self.epoch = Some(*epoch);
                self.committees = committees.clone();
                self.admin_id = Some(*admin_id);
                self.subscriptions.insert(
                    ChannelId {
                        chain_id: *admin_id,
                        name: ADMIN_CHANNEL.into(),
                    },
                    (),
                );
                self.manager = ChainManager::single(*owner);
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}
