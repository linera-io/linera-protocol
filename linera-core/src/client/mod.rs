// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map, BTreeMap, HashMap},
    convert::Infallible,
    iter,
    num::NonZeroUsize,
    ops::Deref,
    sync::Arc,
};

use futures::{
    future::{self, FusedFuture, Future},
    lock::Mutex,
    stream::{self, AbortHandle, FusedStream, FuturesUnordered, StreamExt},
};
use dashmap::DashMap;
use linera_base::{
    abi::Abi,
    crypto::{CryptoHash, KeyPair, PublicKey},
    data_types::{
        Amount, ApplicationPermissions, ArithmeticError, BlockHeight, HashedBlob, Round, Timestamp,
    },
    ensure,
    identifiers::{Account, ApplicationId, BlobId, BytecodeId, ChainId, MessageId, Owner},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_chain::{
    data_types::{
        Block, BlockAndRound, BlockProposal, Certificate, CertificateValue, ExecutedBlock,
        HashedCertificateValue, IncomingMessage, LiteCertificate, LiteVote, MessageAction,
    },
    ChainError, ChainExecutionContext, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    system::{
        AdminOperation, OpenChainConfig, Recipient, SystemChannel, SystemOperation, UserData,
        CREATE_APPLICATION_MESSAGE_INDEX, OPEN_CHAIN_MESSAGE_INDEX, PUBLISH_BYTECODE_MESSAGE_INDEX,
    },
    Bytecode, BytecodeLocation, ExecutionError, Message, Operation, Query, Response,
    SystemExecutionError, SystemMessage, SystemQuery, SystemResponse, UserApplicationId,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use lru::LruCache;
use serde::Serialize;
use thiserror::Error;
use tracing::{debug, error, info};

use crate::{
    data_types::{
        BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse, ClientOutcome, RoundTimeout,
    },
    node::{
        CrossChainMessageDelivery, LocalValidatorNode, LocalValidatorNodeProvider, NodeError,
        NotificationStream, ValidatorNodeProvider,
    },
    value_cache::{CertificateValueCache, DEFAULT_VALUE_CACHE_SIZE},
    worker::{DeliveryNotifiers, Notification, Reason, WorkerError, WorkerState},
};

mod chain;
mod local_node;
mod notifier;
mod updater;

use self::{
    local_node::LocalNodeClient,
    updater::{communicate_with_quorum, CommunicateAction, ValidatorUpdater},
};

pub(crate) use self::{
    local_node::LocalNodeError,
    notifier::Notifier,
    updater::CommunicationError,
};

pub use self::chain::{
    Client as ChainClient,
    Error as ChainClientError,
};

#[cfg(test)]
#[path = "unit_tests/client_tests.rs"]
mod client_tests;

/// The client's view of a chain.
pub struct Chain {
    /// The current state of the chain.
    state: chain::State,

    /// A [`tokio::sync::broadcast::Sender`] to notify clients of notifications on this
    /// chain.
    notification_sender: tokio::sync::broadcast::Sender<Notification>,
}

impl From<chain::State> for Chain {
    fn from(state: chain::State) -> Self {
        Self {
            state,
            notification_sender: tokio::sync::broadcast::Sender::new(NOTIFICATION_QUEUE_SIZE),
        }
    }
}

/*
// TODO make sure all this functionality is covered
pub struct ChainClient<ValidatorNodeProvider, Storage> {
    /// The off-chain chain ID.
    chain_id: ChainId,
    /// How to talk to the validators.
    validator_node_provider: ValidatorNodeProvider,
    /// Latest block hash, if any.
    block_hash: Option<CryptoHash>,
    /// The earliest possible timestamp for the next block.
    timestamp: Timestamp,
    /// Sequence number that we plan to use for the next block.
    /// We track this value outside local storage mainly for security reasons.
    next_block_height: BlockHeight,
    /// Pending block.
    pending_block: Option<Block>,
    /// Known key pairs from present and past identities.
    known_key_pairs: BTreeMap<Owner, KeyPair>,
    /// The ID of the admin chain.
    admin_id: ChainId,

    /// Maximum number of pending messages processed at a time in a block.
    max_pending_messages: usize,
    /// The policy for automatically handling incoming messages.
    message_policy: MessagePolicy,
    /// Whether to block on cross-chain message delivery.
    cross_chain_message_delivery: CrossChainMessageDelivery,
    /// Support synchronization of received certificates.
    received_certificate_trackers: HashMap<ValidatorName, u64>,
    /// Local node to manage the execution state and the local storage of the chains that we are
    /// tracking.
    local_node: LocalNodeClient<Storage>,
    /// This contains blobs belonging to our `pending_block` that may not even have
    /// been processed by (i.e. been proposed to) our own local chain manager yet.
    pending_blobs: BTreeMap<BlobId, HashedBlob>,
}
*/

const NOTIFICATION_QUEUE_SIZE: usize = 4096;

/// A client to the Linera protocol.
pub struct Client<ValidatorNodeProvider, Storage> {
    /// How to talk to the validators.
    validator_node_provider: ValidatorNodeProvider,
    /// Per-chain information.
    chains: DashMap<ChainId, Chain>,
    /// The ID of the admin chain.
    admin_chain_id: ChainId,

    /// Client storage (used by the updater).
    storage: Storage,

    /// Local node to manage the execution state and the local storage of the chains that we are
    /// tracking.
    local_node: LocalNodeClient<Storage>,
    /// Known key pairs from present and past identities.
    known_key_pairs: DashMap<Owner, KeyPair>,
}

impl<ValidatorNodeProvider, Storage: Clone> Client<ValidatorNodeProvider, Storage> {
    fn new(
        admin_chain_id: ChainId,
        validator_node_provider: ValidatorNodeProvider,
        storage: Storage,
    ) -> Self {
        Self {
            validator_node_provider,
            chains: Default::default(),
            admin_chain_id,
            storage: storage.clone(),
            local_node: LocalNodeClient::new(
                WorkerState::new_for_client(
                    format!("Client node"),
                    storage,
                ).with_allow_inactive_chains(true).with_allow_messages_from_deprecated_epochs(true),
            ),
            known_key_pairs: Default::default(),
        }
    }

    fn chain(&self, id: ChainId) -> Option<ChainClient<ValidatorNodeProvider, Storage>> {
        Some(ChainClient {
            client: self,
            chain_id: id,
            options: chain::Options::default(),
        }).filter(|_| self.chains.contains_key(&id))
    }

    fn admin_chain(&self) -> ChainClient<ValidatorNodeProvider, Storage> {
        self.chain(self.admin_chain_id).expect("we should know about the admin chain")
    }

    /// Create a new chain.
    ///
    /// This is not intended to be user-facing; instead, read this information from the
    /// wallet using `linera-client`.
    fn create_chain(&self, state: chain::State) -> Option<chain::State> {
        use dashmap::mapref::entry::Entry::*;
        match self.chains.entry(state.id) {
            Occupied(_) => Some(state),
            Vacant(entry) => {
                entry.insert(state.into());
                None
            }
        }
    }

    fn chain_or_create(&self, state: chain::State) -> ChainClient<ValidatorNodeProvider, Storage> {
        chain::Client {
            client: self,
            chain_id: self.chains.entry(state.id.clone()).or_insert_with(|| {
                Chain {
                    state,
                    notification_sender: tokio::sync::broadcast::Sender::new(NOTIFICATION_QUEUE_SIZE),
                }
            }).key().clone(),
            options: chain::Options::default(),
        }
    }

    fn dispatch_notification(&self, notification: Notification) {
        if let Some(chain) = self.chains.get(&notification.chain_id) {
            // An `Err` here only implies that nobody is listening for this
            // notification.
            let _ = chain.notification_sender.send(notification);
        }
    }
}

/// Wrapper for `AbortHandle` that aborts when its dropped.
#[must_use]
pub struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
