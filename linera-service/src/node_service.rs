// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, future::IntoFuture, iter, net::SocketAddr, num::NonZeroU16, sync::Arc};

use async_graphql::{
    futures_util::Stream, resolver_utils::ContainerType, EmptyMutation, Error, MergedObject,
    OutputType, Request, Response, ScalarType, Schema, SimpleObject, Subscription,
};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{extract::Path, http::StatusCode, response, response::IntoResponse, Extension, Router};
use futures::{lock::Mutex, Future, FutureExt as _, TryStreamExt as _};
use linera_base::{
    crypto::{CryptoError, CryptoHash},
    data_types::{
        Amount, ApplicationDescription, ApplicationPermissions, Bytecode, Epoch, TimeDelta,
    },
    identifiers::{
        Account, AccountOwner, ApplicationId, ChainId, IndexAndEvent, ModuleId, StreamId,
    },
    ownership::{ChainOwnership, TimeoutConfig},
    vm::VmRuntime,
    BcsHexParseError,
};
use linera_chain::{
    types::{ConfirmedBlock, GenericCertificate},
    ChainStateView,
};
use linera_client::chain_listener::{
    ChainListener, ChainListenerConfig, ClientContext, ListenerCommand,
};
use linera_core::{
    client::chain_client::{self, ChainClient},
    data_types::ClientOutcome,
    wallet::Wallet as _,
    worker::Notification,
};
use linera_execution::{
    committee::Committee, system::AdminOperation, Operation, Query, QueryOutcome, QueryResponse,
    SystemOperation,
};
#[cfg(with_metrics)]
use linera_metrics::monitoring_server;
use linera_sdk::linera_base_types::BlobContent;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{mpsc::UnboundedReceiver, OwnedRwLockReadGuard};
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;
use tracing::{debug, info, instrument, trace};

use crate::util;

#[derive(SimpleObject, Serialize, Deserialize, Clone)]
pub struct Chains {
    pub list: Vec<ChainId>,
    pub default: Option<ChainId>,
}

/// Our root GraphQL query type.
pub struct QueryRoot<C> {
    context: Arc<Mutex<C>>,
    port: NonZeroU16,
    default_chain: Option<ChainId>,
}

/// Our root GraphQL subscription type.
pub struct SubscriptionRoot<C> {
    context: Arc<Mutex<C>>,
}

/// Our root GraphQL mutation type.
pub struct MutationRoot<C> {
    context: Arc<Mutex<C>>,
}

#[derive(Debug, thiserror::Error)]
enum NodeServiceError {
    #[error(transparent)]
    ChainClient(#[from] chain_client::Error),
    #[error(transparent)]
    BcsHex(#[from] BcsHexParseError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("malformed chain ID: {0}")]
    InvalidChainId(CryptoError),
    #[error(transparent)]
    Client(#[from] linera_client::Error),
    #[error("scheduling operations from queries is disabled in read-only mode")]
    ReadOnlyModeOperationsNotAllowed,
}

impl IntoResponse for NodeServiceError {
    fn into_response(self) -> response::Response {
        let status = match self {
            NodeServiceError::InvalidChainId(_) | NodeServiceError::BcsHex(_) => {
                StatusCode::BAD_REQUEST
            }
            NodeServiceError::ReadOnlyModeOperationsNotAllowed => StatusCode::FORBIDDEN,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = json!({"error": self.to_string()}).to_string();
        (status, body).into_response()
    }
}

#[Subscription]
impl<C> SubscriptionRoot<C>
where
    C: ClientContext + 'static,
{
    /// Subscribes to notifications from the specified chain.
    async fn notifications(
        &self,
        chain_id: ChainId,
    ) -> Result<impl Stream<Item = Notification>, Error> {
        let client = self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .await?;
        Ok(client.subscribe()?)
    }
}

impl<C> MutationRoot<C>
where
    C: ClientContext,
{
    async fn execute_system_operation(
        &self,
        system_operation: SystemOperation,
        chain_id: ChainId,
    ) -> Result<CryptoHash, Error> {
        let certificate = self
            .apply_client_command(&chain_id, move |client| {
                let operation = Operation::system(system_operation.clone());
                async move {
                    let result = client
                        .execute_operation(operation)
                        .await
                        .map_err(Error::from);
                    (result, client)
                }
            })
            .await?;
        Ok(certificate.hash())
    }

    /// Applies the given function to the chain client.
    /// Updates the wallet regardless of the outcome. As long as the function returns a round
    /// timeout, it will wait and retry.
    async fn apply_client_command<F, Fut, T>(
        &self,
        chain_id: &ChainId,
        mut f: F,
    ) -> Result<T, Error>
    where
        F: FnMut(ChainClient<C::Environment>) -> Fut,
        Fut: Future<Output = (Result<ClientOutcome<T>, Error>, ChainClient<C::Environment>)>,
    {
        loop {
            let client = self
                .context
                .lock()
                .await
                .make_chain_client(*chain_id)
                .await?;
            let mut stream = client.subscribe()?;
            let (result, client) = f(client).await;
            self.context.lock().await.update_wallet(&client).await?;
            let timeout = match result? {
                ClientOutcome::Committed(t) => return Ok(t),
                ClientOutcome::Conflict(certificate) => {
                    return Err(chain_client::Error::Conflict(certificate.hash()).into());
                }
                ClientOutcome::WaitForTimeout(timeout) => timeout,
            };
            drop(client);
            util::wait_for_next_round(&mut stream, timeout).await;
        }
    }
}

#[async_graphql::Object(cache_control(no_cache))]
impl<C> MutationRoot<C>
where
    C: ClientContext + 'static,
{
    /// Processes the inbox and returns the lists of certificate hashes that were created, if any.
    async fn process_inbox(
        &self,
        #[graphql(desc = "The chain whose inbox is being processed.")] chain_id: ChainId,
    ) -> Result<Vec<CryptoHash>, Error> {
        let mut hashes = Vec::new();
        loop {
            let client = self
                .context
                .lock()
                .await
                .make_chain_client(chain_id)
                .await?;
            let result = client.process_inbox().await;
            self.context.lock().await.update_wallet(&client).await?;
            let (certificates, maybe_timeout) = result?;
            hashes.extend(certificates.into_iter().map(|cert| cert.hash()));
            match maybe_timeout {
                None => return Ok(hashes),
                Some(timestamp) => {
                    let mut stream = client.subscribe()?;
                    drop(client);
                    util::wait_for_next_round(&mut stream, timestamp).await;
                }
            }
        }
    }

    /// Synchronizes the chain with the validators. Returns the chain's length.
    ///
    /// This is only used for testing, to make sure that a client is up to date.
    // TODO(#4718): Remove this mutation.
    async fn sync(
        &self,
        #[graphql(desc = "The chain being synchronized.")] chain_id: ChainId,
    ) -> Result<u64, Error> {
        let client = self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .await?;
        let info = client.synchronize_from_validators().await?;
        self.context.lock().await.update_wallet(&client).await?;
        Ok(info.next_block_height.0)
    }

    /// Retries the pending block that was unsuccessfully proposed earlier.
    async fn retry_pending_block(
        &self,
        #[graphql(desc = "The chain on whose block is being retried.")] chain_id: ChainId,
    ) -> Result<Option<CryptoHash>, Error> {
        let client = self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .await?;
        let outcome = client.process_pending_block().await?;
        self.context.lock().await.update_wallet(&client).await?;
        match outcome {
            ClientOutcome::Committed(Some(certificate)) => Ok(Some(certificate.hash())),
            ClientOutcome::Committed(None) => Ok(None),
            ClientOutcome::WaitForTimeout(timeout) => Err(Error::from(format!(
                "Please try again at {}",
                timeout.timestamp
            ))),
            ClientOutcome::Conflict(certificate) => Err(Error::from(format!(
                "A different block was committed: {}",
                certificate.hash()
            ))),
        }
    }

    /// Transfers `amount` units of value from the given owner's account to the recipient.
    /// If no owner is given, try to take the units out of the chain account.
    async fn transfer(
        &self,
        #[graphql(desc = "The chain which native tokens are being transferred from.")]
        chain_id: ChainId,
        #[graphql(desc = "The account being debited on the chain.")] owner: AccountOwner,
        #[graphql(desc = "The recipient of the transfer.")] recipient: Account,
        #[graphql(desc = "The amount being transferred.")] amount: Amount,
    ) -> Result<CryptoHash, Error> {
        self.apply_client_command(&chain_id, move |client| async move {
            let result = client
                .transfer(owner, amount, recipient)
                .await
                .map_err(Error::from)
                .map(|outcome| outcome.map(|certificate| certificate.hash()));
            (result, client)
        })
        .await
    }

    /// Claims `amount` units of value from the given owner's account in the remote
    /// `target` chain. Depending on its configuration, the `target` chain may refuse to
    /// process the message.
    async fn claim(
        &self,
        #[graphql(desc = "The chain for whom owner is one of the owner.")] chain_id: ChainId,
        #[graphql(desc = "The owner of chain targetId being debited.")] owner: AccountOwner,
        #[graphql(desc = "The chain whose owner is being debited.")] target_id: ChainId,
        #[graphql(desc = "The recipient of the transfer.")] recipient: Account,
        #[graphql(desc = "The amount being transferred.")] amount: Amount,
    ) -> Result<CryptoHash, Error> {
        self.apply_client_command(&chain_id, move |client| async move {
            let result = client
                .claim(owner, target_id, recipient, amount)
                .await
                .map_err(Error::from)
                .map(|outcome| outcome.map(|certificate| certificate.hash()));
            (result, client)
        })
        .await
    }

    /// Test if a data blob is readable from a transaction in the current chain.
    // TODO(#2490): Consider removing or renaming this.
    async fn read_data_blob(
        &self,
        chain_id: ChainId,
        hash: CryptoHash,
    ) -> Result<CryptoHash, Error> {
        self.apply_client_command(&chain_id, move |client| async move {
            let result = client
                .read_data_blob(hash)
                .await
                .map_err(Error::from)
                .map(|outcome| outcome.map(|certificate| certificate.hash()));
            (result, client)
        })
        .await
    }

    /// Creates a new single-owner chain.
    async fn open_chain(
        &self,
        #[graphql(desc = "The chain paying for the creation of the new chain.")] chain_id: ChainId,
        #[graphql(desc = "The owner of the new chain.")] owner: AccountOwner,
        #[graphql(desc = "The balance of the chain being created. Zero if `None`.")]
        balance: Option<Amount>,
    ) -> Result<ChainId, Error> {
        let ownership = ChainOwnership::single(owner);
        let balance = balance.unwrap_or(Amount::ZERO);
        let description = self
            .apply_client_command(&chain_id, move |client| {
                let ownership = ownership.clone();
                async move {
                    let result = client
                        .open_chain(ownership, ApplicationPermissions::default(), balance)
                        .await
                        .map_err(Error::from)
                        .map(|outcome| outcome.map(|(chain_id, _)| chain_id));
                    (result, client)
                }
            })
            .await?;
        Ok(description.id())
    }

    /// Creates a new multi-owner chain.
    #[expect(clippy::too_many_arguments)]
    async fn open_multi_owner_chain(
        &self,
        #[graphql(desc = "The chain paying for the creation of the new chain.")] chain_id: ChainId,
        #[graphql(desc = "Permissions for applications on the new chain")]
        application_permissions: Option<ApplicationPermissions>,
        #[graphql(desc = "The owners of the chain")] owners: Vec<AccountOwner>,
        #[graphql(desc = "The weights of the owners")] weights: Option<Vec<u64>>,
        #[graphql(desc = "The number of multi-leader rounds")] multi_leader_rounds: Option<u32>,
        #[graphql(desc = "The balance of the chain. Zero if `None`")] balance: Option<Amount>,
        #[graphql(desc = "The duration of the fast round, in milliseconds; default: no timeout")]
        fast_round_ms: Option<u64>,
        #[graphql(
            desc = "The duration of the first single-leader and all multi-leader rounds",
            default = 10_000
        )]
        base_timeout_ms: u64,
        #[graphql(
            desc = "The number of milliseconds by which the timeout increases after each \
                    single-leader round",
            default = 1_000
        )]
        timeout_increment_ms: u64,
        #[graphql(
            desc = "The age of an incoming tracked or protected message after which the \
                    validators start transitioning the chain to fallback mode, in milliseconds.",
            default = 86_400_000
        )]
        fallback_duration_ms: u64,
    ) -> Result<ChainId, Error> {
        let owners = if let Some(weights) = weights {
            if weights.len() != owners.len() {
                return Err(Error::new(format!(
                    "There are {} owners but {} weights.",
                    owners.len(),
                    weights.len()
                )));
            }
            owners.into_iter().zip(weights).collect::<Vec<_>>()
        } else {
            owners
                .into_iter()
                .zip(iter::repeat(100))
                .collect::<Vec<_>>()
        };
        let multi_leader_rounds = multi_leader_rounds.unwrap_or(u32::MAX);
        let timeout_config = TimeoutConfig {
            fast_round_duration: fast_round_ms.map(TimeDelta::from_millis),
            base_timeout: TimeDelta::from_millis(base_timeout_ms),
            timeout_increment: TimeDelta::from_millis(timeout_increment_ms),
            fallback_duration: TimeDelta::from_millis(fallback_duration_ms),
        };
        let ownership = ChainOwnership::multiple(owners, multi_leader_rounds, timeout_config);
        let balance = balance.unwrap_or(Amount::ZERO);
        let description = self
            .apply_client_command(&chain_id, move |client| {
                let ownership = ownership.clone();
                let application_permissions = application_permissions.clone().unwrap_or_default();
                async move {
                    let result = client
                        .open_chain(ownership, application_permissions, balance)
                        .await
                        .map_err(Error::from)
                        .map(|outcome| outcome.map(|(chain_id, _)| chain_id));
                    (result, client)
                }
            })
            .await?;
        Ok(description.id())
    }

    /// Closes the chain. Returns the new block hash if successful or `None` if it was already closed.
    async fn close_chain(
        &self,
        #[graphql(desc = "The chain being closed.")] chain_id: ChainId,
    ) -> Result<Option<CryptoHash>, Error> {
        let maybe_cert = self
            .apply_client_command(&chain_id, |client| async move {
                let result = client.close_chain().await.map_err(Error::from);
                (result, client)
            })
            .await?;
        Ok(maybe_cert.as_ref().map(GenericCertificate::hash))
    }

    /// Changes the chain to a single-owner chain
    async fn change_owner(
        &self,
        #[graphql(desc = "The chain whose ownership changes")] chain_id: ChainId,
        #[graphql(desc = "The new single owner of the chain")] new_owner: AccountOwner,
    ) -> Result<CryptoHash, Error> {
        let operation = SystemOperation::ChangeOwnership {
            super_owners: vec![new_owner],
            owners: Vec::new(),
            first_leader: None,
            multi_leader_rounds: 2,
            open_multi_leader_rounds: false,
            timeout_config: TimeoutConfig::default(),
        };
        self.execute_system_operation(operation, chain_id).await
    }

    /// Changes the ownership of the chain
    #[expect(clippy::too_many_arguments)]
    async fn change_multiple_owners(
        &self,
        #[graphql(desc = "The chain whose ownership changes")] chain_id: ChainId,
        #[graphql(desc = "The new list of owners of the chain")] new_owners: Vec<AccountOwner>,
        #[graphql(desc = "The new list of weights of the owners")] new_weights: Vec<u64>,
        #[graphql(desc = "The multi-leader round of the chain")] multi_leader_rounds: u32,
        #[graphql(
            desc = "Whether multi-leader rounds are unrestricted, that is not limited to chain owners."
        )]
        open_multi_leader_rounds: bool,
        #[graphql(desc = "The leader of the first single-leader round. \
                          If not set, this is random like other rounds.")]
        first_leader: Option<AccountOwner>,
        #[graphql(desc = "The duration of the fast round, in milliseconds; default: no timeout")]
        fast_round_ms: Option<u64>,
        #[graphql(
            desc = "The duration of the first single-leader and all multi-leader rounds",
            default = 10_000
        )]
        base_timeout_ms: u64,
        #[graphql(
            desc = "The number of milliseconds by which the timeout increases after each \
                    single-leader round",
            default = 1_000
        )]
        timeout_increment_ms: u64,
        #[graphql(
            desc = "The age of an incoming tracked or protected message after which the \
                    validators start transitioning the chain to fallback mode, in milliseconds.",
            default = 86_400_000
        )]
        fallback_duration_ms: u64,
    ) -> Result<CryptoHash, Error> {
        let operation = SystemOperation::ChangeOwnership {
            super_owners: Vec::new(),
            owners: new_owners.into_iter().zip(new_weights).collect(),
            first_leader,
            multi_leader_rounds,
            open_multi_leader_rounds,
            timeout_config: TimeoutConfig {
                fast_round_duration: fast_round_ms.map(TimeDelta::from_millis),
                base_timeout: TimeDelta::from_millis(base_timeout_ms),
                timeout_increment: TimeDelta::from_millis(timeout_increment_ms),
                fallback_duration: TimeDelta::from_millis(fallback_duration_ms),
            },
        };
        self.execute_system_operation(operation, chain_id).await
    }

    /// Changes the application permissions configuration on this chain.
    #[expect(clippy::too_many_arguments)]
    async fn change_application_permissions(
        &self,
        #[graphql(desc = "The chain whose permissions are being changed")] chain_id: ChainId,
        #[graphql(
            desc = "These applications are allowed to manage the chain: close it, change \
                    application permissions, and change ownership."
        )]
        manage_chain: Vec<ApplicationId>,
        #[graphql(
            desc = "If this is `None`, all system operations and application operations are allowed.
If it is `Some`, only operations from the specified applications are allowed,
and no system operations."
        )]
        execute_operations: Option<Vec<ApplicationId>>,
        #[graphql(
            desc = "At least one operation or incoming message from each of these applications must occur in every block."
        )]
        mandatory_applications: Vec<ApplicationId>,
        #[graphql(
            desc = "These applications are allowed to perform calls to services as oracles."
        )]
        call_service_as_oracle: Option<Vec<ApplicationId>>,
        #[graphql(desc = "These applications are allowed to perform HTTP requests.")]
        make_http_requests: Option<Vec<ApplicationId>>,
    ) -> Result<CryptoHash, Error> {
        let operation = SystemOperation::ChangeApplicationPermissions(ApplicationPermissions {
            execute_operations,
            mandatory_applications,
            manage_chain,
            call_service_as_oracle,
            make_http_requests,
        });
        self.execute_system_operation(operation, chain_id).await
    }

    /// (admin chain only) Registers a new committee. This will notify the subscribers of
    /// the admin chain so that they can migrate to the new epoch (by accepting the
    /// notification as an "incoming message" in a next block).
    async fn create_committee(
        &self,
        chain_id: ChainId,
        committee: Committee,
    ) -> Result<CryptoHash, Error> {
        Ok(self
            .apply_client_command(&chain_id, move |client| {
                let committee = committee.clone();
                async move {
                    let result = client
                        .stage_new_committee(committee)
                        .await
                        .map_err(Error::from);
                    (result, client)
                }
            })
            .await?
            .hash())
    }

    /// (admin chain only) Removes a committee. Once this message is accepted by a chain,
    /// blocks from the retired epoch will not be accepted until they are followed (hence
    /// re-certified) by a block certified by a recent committee.
    async fn remove_committee(&self, chain_id: ChainId, epoch: Epoch) -> Result<CryptoHash, Error> {
        let operation = SystemOperation::Admin(AdminOperation::RemoveCommittee { epoch });
        self.execute_system_operation(operation, chain_id).await
    }

    /// Publishes a new application module.
    async fn publish_module(
        &self,
        #[graphql(desc = "The chain publishing the module")] chain_id: ChainId,
        #[graphql(desc = "The bytecode of the contract code")] contract: Bytecode,
        #[graphql(desc = "The bytecode of the service code (only relevant for WebAssembly)")]
        service: Bytecode,
        #[graphql(desc = "The virtual machine being used (either Wasm or Evm)")]
        vm_runtime: VmRuntime,
    ) -> Result<ModuleId, Error> {
        self.apply_client_command(&chain_id, move |client| {
            let contract = contract.clone();
            let service = service.clone();
            async move {
                let result = client
                    .publish_module(contract, service, vm_runtime)
                    .await
                    .map_err(Error::from)
                    .map(|outcome| outcome.map(|(module_id, _)| module_id));
                (result, client)
            }
        })
        .await
    }

    /// Publishes a new data blob.
    async fn publish_data_blob(
        &self,
        #[graphql(desc = "The chain paying for the blob publication")] chain_id: ChainId,
        #[graphql(desc = "The content of the data blob being created")] bytes: Vec<u8>,
    ) -> Result<CryptoHash, Error> {
        self.apply_client_command(&chain_id, |client| {
            let bytes = bytes.clone();
            async move {
                let result = client.publish_data_blob(bytes).await.map_err(Error::from);
                (result, client)
            }
        })
        .await
        .map(|_| CryptoHash::new(&BlobContent::new_data(bytes)))
    }

    /// Creates a new application.
    async fn create_application(
        &self,
        #[graphql(desc = "The chain paying for the creation of the application")] chain_id: ChainId,
        #[graphql(desc = "The module ID of the application being created")] module_id: ModuleId,
        #[graphql(desc = "The JSON serialization of the parameters of the application")]
        parameters: String,
        #[graphql(
            desc = "The JSON serialization of the instantiation argument of the application"
        )]
        instantiation_argument: String,
        #[graphql(desc = "The dependencies of the application being created")]
        required_application_ids: Vec<ApplicationId>,
    ) -> Result<ApplicationId, Error> {
        self.apply_client_command(&chain_id, move |client| {
            let parameters = parameters.as_bytes().to_vec();
            let instantiation_argument = instantiation_argument.as_bytes().to_vec();
            let required_application_ids = required_application_ids.clone();
            async move {
                let result = client
                    .create_application_untyped(
                        module_id,
                        parameters,
                        instantiation_argument,
                        required_application_ids,
                    )
                    .await
                    .map_err(Error::from)
                    .map(|outcome| outcome.map(|(application_id, _)| application_id));
                (result, client)
            }
        })
        .await
    }
}

#[async_graphql::Object(cache_control(no_cache))]
impl<C> QueryRoot<C>
where
    C: ClientContext + 'static,
{
    async fn chain(
        &self,
        chain_id: ChainId,
    ) -> Result<
        ChainStateExtendedView<<C::Environment as linera_core::Environment>::StorageContext>,
        Error,
    > {
        let client = self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .await?;
        let view = client.chain_state_view().await?;
        Ok(ChainStateExtendedView::new(view))
    }

    async fn applications(&self, chain_id: ChainId) -> Result<Vec<ApplicationOverview>, Error> {
        let client = self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .await?;
        let applications = client
            .chain_state_view()
            .await?
            .execution_state
            .list_applications()
            .await?;

        let overviews = applications
            .into_iter()
            .map(|(id, description)| ApplicationOverview::new(id, description, self.port, chain_id))
            .collect();

        Ok(overviews)
    }

    async fn chains(&self) -> Result<Chains, Error> {
        Ok(Chains {
            list: self
                .context
                .lock()
                .await
                .wallet()
                .chain_ids()
                .try_collect()
                .await?,
            default: self.default_chain,
        })
    }

    async fn block(
        &self,
        hash: Option<CryptoHash>,
        chain_id: ChainId,
    ) -> Result<Option<ConfirmedBlock>, Error> {
        let client = self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .await?;
        let hash = match hash {
            Some(hash) => Some(hash),
            None => client.chain_info().await?.block_hash,
        };
        if let Some(hash) = hash {
            let block = client.read_confirmed_block(hash).await?;
            Ok(Some(block))
        } else {
            Ok(None)
        }
    }

    async fn events_from_index(
        &self,
        chain_id: ChainId,
        stream_id: StreamId,
        start_index: u32,
    ) -> Result<Vec<IndexAndEvent>, Error> {
        Ok(self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .await?
            .events_from_index(stream_id, start_index)
            .await?)
    }

    async fn blocks(
        &self,
        from: Option<CryptoHash>,
        chain_id: ChainId,
        limit: Option<u32>,
    ) -> Result<Vec<ConfirmedBlock>, Error> {
        let client = self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .await?;
        let limit = limit.unwrap_or(10);
        let from = match from {
            Some(from) => Some(from),
            None => client.chain_info().await?.block_hash,
        };
        let Some(from) = from else {
            return Ok(vec![]);
        };
        let mut hash = Some(from);
        let mut values = Vec::new();
        for _ in 0..limit {
            let Some(next_hash) = hash else {
                break;
            };
            let value = client.read_confirmed_block(next_hash).await?;
            hash = value.block().header.previous_block_hash;
            values.push(value);
        }
        Ok(values)
    }

    /// Returns the version information on this node service.
    async fn version(&self) -> linera_version::VersionInfo {
        linera_version::VersionInfo::default()
    }
}

// What follows is a hack to add a chain_id field to `ChainStateView` based on
// https://async-graphql.github.io/async-graphql/en/merging_objects.html

struct ChainStateViewExtension(ChainId);

#[async_graphql::Object(cache_control(no_cache))]
impl ChainStateViewExtension {
    async fn chain_id(&self) -> ChainId {
        self.0
    }
}

#[derive(MergedObject)]
struct ChainStateExtendedView<C>(ChainStateViewExtension, ReadOnlyChainStateView<C>)
where
    C: linera_views::context::Context + Clone + Send + Sync + 'static,
    C::Extra: linera_execution::ExecutionRuntimeContext;

/// A wrapper type that allows proxying GraphQL queries to a [`ChainStateView`] that's behind an
/// [`OwnedRwLockReadGuard`].
pub struct ReadOnlyChainStateView<C>(OwnedRwLockReadGuard<ChainStateView<C>>)
where
    C: linera_views::context::Context + Clone + Send + Sync + 'static;

impl<C> ContainerType for ReadOnlyChainStateView<C>
where
    C: linera_views::context::Context + Clone + Send + Sync + 'static,
{
    async fn resolve_field(
        &self,
        context: &async_graphql::Context<'_>,
    ) -> async_graphql::ServerResult<Option<async_graphql::Value>> {
        self.0.resolve_field(context).await
    }
}

impl<C> OutputType for ReadOnlyChainStateView<C>
where
    C: linera_views::context::Context + Clone + Send + Sync + 'static,
{
    fn type_name() -> Cow<'static, str> {
        ChainStateView::<C>::type_name()
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        ChainStateView::<C>::create_type_info(registry)
    }

    async fn resolve(
        &self,
        context: &async_graphql::ContextSelectionSet<'_>,
        field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
    ) -> async_graphql::ServerResult<async_graphql::Value> {
        self.0.resolve(context, field).await
    }
}

impl<C> ChainStateExtendedView<C>
where
    C: linera_views::context::Context + Clone + Send + Sync + 'static,
    C::Extra: linera_execution::ExecutionRuntimeContext,
{
    fn new(view: OwnedRwLockReadGuard<ChainStateView<C>>) -> Self {
        Self(
            ChainStateViewExtension(view.chain_id()),
            ReadOnlyChainStateView(view),
        )
    }
}

#[derive(SimpleObject)]
pub struct ApplicationOverview {
    id: ApplicationId,
    description: ApplicationDescription,
    link: String,
}

impl ApplicationOverview {
    fn new(
        id: ApplicationId,
        description: ApplicationDescription,
        port: NonZeroU16,
        chain_id: ChainId,
    ) -> Self {
        Self {
            id,
            description,
            link: format!(
                "http://localhost:{}/chains/{}/applications/{}",
                port.get(),
                chain_id,
                id
            ),
        }
    }
}

/// Schema type that can be either full (with mutations) or read-only.
pub enum NodeServiceSchema<C>
where
    C: ClientContext + 'static,
{
    /// Full schema with mutations enabled.
    Full(Schema<QueryRoot<C>, MutationRoot<C>, SubscriptionRoot<C>>),
    /// Read-only schema with mutations disabled.
    ReadOnly(Schema<QueryRoot<C>, EmptyMutation, SubscriptionRoot<C>>),
}

impl<C> NodeServiceSchema<C>
where
    C: ClientContext,
{
    /// Executes a GraphQL request.
    pub async fn execute(&self, request: impl Into<Request>) -> Response {
        match self {
            Self::Full(schema) => schema.execute(request).await,
            Self::ReadOnly(schema) => schema.execute(request).await,
        }
    }

    /// Returns the SDL (Schema Definition Language) representation.
    pub fn sdl(&self) -> String {
        match self {
            Self::Full(schema) => schema.sdl(),
            Self::ReadOnly(schema) => schema.sdl(),
        }
    }
}

impl<C> Clone for NodeServiceSchema<C>
where
    C: ClientContext,
{
    fn clone(&self) -> Self {
        match self {
            Self::Full(schema) => Self::Full(schema.clone()),
            Self::ReadOnly(schema) => Self::ReadOnly(schema.clone()),
        }
    }
}

/// The `NodeService` is a server that exposes a web-server to the client.
/// The node service is primarily used to explore the state of a chain in GraphQL.
pub struct NodeService<C>
where
    C: ClientContext + 'static,
{
    config: ChainListenerConfig,
    port: NonZeroU16,
    #[cfg(with_metrics)]
    metrics_port: NonZeroU16,
    default_chain: Option<ChainId>,
    context: Arc<Mutex<C>>,
    /// If true, disallow mutations and prevent queries from scheduling operations.
    read_only: bool,
}

impl<C> Clone for NodeService<C>
where
    C: ClientContext + 'static,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            port: self.port,
            #[cfg(with_metrics)]
            metrics_port: self.metrics_port,
            default_chain: self.default_chain,
            context: Arc::clone(&self.context),
            read_only: self.read_only,
        }
    }
}

impl<C> NodeService<C>
where
    C: ClientContext,
{
    /// Creates a new instance of the node service given a client chain and a port.
    pub fn new(
        config: ChainListenerConfig,
        port: NonZeroU16,
        #[cfg(with_metrics)] metrics_port: NonZeroU16,
        default_chain: Option<ChainId>,
        context: Arc<Mutex<C>>,
        read_only: bool,
    ) -> Self {
        Self {
            config,
            port,
            #[cfg(with_metrics)]
            metrics_port,
            default_chain,
            context,
            read_only,
        }
    }

    #[cfg(with_metrics)]
    pub fn metrics_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.metrics_port.get()))
    }

    pub fn schema(&self) -> NodeServiceSchema<C> {
        let query = QueryRoot {
            context: Arc::clone(&self.context),
            port: self.port,
            default_chain: self.default_chain,
        };
        let subscription = SubscriptionRoot {
            context: Arc::clone(&self.context),
        };

        if self.read_only {
            NodeServiceSchema::ReadOnly(Schema::build(query, EmptyMutation, subscription).finish())
        } else {
            NodeServiceSchema::Full(
                Schema::build(
                    query,
                    MutationRoot {
                        context: Arc::clone(&self.context),
                    },
                    subscription,
                )
                .finish(),
            )
        }
    }

    /// Runs the node service.
    #[instrument(name = "node_service", level = "info", skip_all, fields(port = ?self.port))]
    pub async fn run(
        self,
        cancellation_token: CancellationToken,
        command_receiver: UnboundedReceiver<ListenerCommand>,
    ) -> Result<(), anyhow::Error> {
        let port = self.port.get();
        let index_handler = axum::routing::get(util::graphiql).post(Self::index_handler);
        let application_handler =
            axum::routing::get(util::graphiql).post(Self::application_handler);

        #[cfg(with_metrics)]
        monitoring_server::start_metrics(self.metrics_address(), cancellation_token.clone());

        let base_router = Router::new()
            .route("/", index_handler)
            .route(
                "/chains/{chain_id}/applications/{application_id}",
                application_handler,
            )
            .route("/ready", axum::routing::get(|| async { "ready!" }));

        // Create router with appropriate schema for WebSocket subscriptions.
        let app = match self.schema() {
            NodeServiceSchema::Full(schema) => {
                base_router.route_service("/ws", GraphQLSubscription::new(schema))
            }
            NodeServiceSchema::ReadOnly(schema) => {
                base_router.route_service("/ws", GraphQLSubscription::new(schema))
            }
        }
        .layer(Extension(self.clone()))
        // TODO(#551): Provide application authentication.
        .layer(CorsLayer::permissive());

        info!("GraphiQL IDE: http://localhost:{}", port);

        let storage = self.context.lock().await.storage().clone();

        let chain_listener = ChainListener::new(
            self.config,
            self.context,
            storage,
            cancellation_token.clone(),
            command_receiver,
            true,
        )
        .run()
        .await?;
        let mut chain_listener = Box::pin(chain_listener).fuse();
        let tcp_listener =
            tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?;
        let server = axum::serve(tcp_listener, app)
            .with_graceful_shutdown(cancellation_token.cancelled_owned())
            .into_future();
        futures::select! {
            result = chain_listener => result?,
            result = Box::pin(server).fuse() => result?,
        };

        Ok(())
    }

    /// Handles service queries for user applications (including mutations).
    async fn handle_service_request(
        &self,
        application_id: ApplicationId,
        request: Vec<u8>,
        chain_id: ChainId,
        block_hash: Option<CryptoHash>,
    ) -> Result<Vec<u8>, NodeServiceError> {
        let QueryOutcome {
            response,
            operations,
        } = self
            .query_user_application(application_id, request, chain_id, block_hash)
            .await?;
        if operations.is_empty() {
            return Ok(response);
        }

        if self.read_only {
            return Err(NodeServiceError::ReadOnlyModeOperationsNotAllowed);
        }

        trace!("Query requested a new block with operations: {operations:?}");
        let client = self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .await?;
        let hash = loop {
            let timeout = match client
                .execute_operations(operations.clone(), vec![])
                .await?
            {
                ClientOutcome::Committed(certificate) => break certificate.hash(),
                ClientOutcome::Conflict(certificate) => {
                    return Err(chain_client::Error::Conflict(certificate.hash()).into());
                }
                ClientOutcome::WaitForTimeout(timeout) => timeout,
            };
            let mut stream = client.subscribe().map_err(|_| {
                chain_client::Error::InternalError("Could not subscribe to the local node.")
            })?;
            util::wait_for_next_round(&mut stream, timeout).await;
        };
        let response = async_graphql::Response::new(hash.to_value());
        Ok(serde_json::to_vec(&response)?)
    }

    /// Queries a user application, returning the raw [`QueryOutcome`].
    async fn query_user_application(
        &self,
        application_id: ApplicationId,
        bytes: Vec<u8>,
        chain_id: ChainId,
        block_hash: Option<CryptoHash>,
    ) -> Result<QueryOutcome<Vec<u8>>, NodeServiceError> {
        let query = Query::User {
            application_id,
            bytes,
        };
        let client = self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .await?;
        let QueryOutcome {
            response,
            operations,
        } = client.query_application(query, block_hash).await?;
        match response {
            QueryResponse::System(_) => {
                unreachable!("cannot get a system response for a user query")
            }
            QueryResponse::User(user_response_bytes) => Ok(QueryOutcome {
                response: user_response_bytes,
                operations,
            }),
        }
    }

    /// Executes a GraphQL query and generates a response for our `Schema`.
    async fn index_handler(service: Extension<Self>, request: GraphQLRequest) -> GraphQLResponse {
        service
            .0
            .schema()
            .execute(request.into_inner())
            .await
            .into()
    }

    /// Executes a GraphQL query against an application.
    /// Pattern matches on the `OperationType` of the query and routes the query
    /// accordingly.
    async fn application_handler(
        Path((chain_id, application_id)): Path<(String, String)>,
        service: Extension<Self>,
        request: String,
    ) -> Result<Vec<u8>, NodeServiceError> {
        let chain_id: ChainId = chain_id.parse().map_err(NodeServiceError::InvalidChainId)?;
        let application_id: ApplicationId = application_id.parse()?;

        debug!(
            %chain_id,
            %application_id,
            "processing request for application:\n{:?}",
            &request
        );
        let response = service
            .0
            .handle_service_request(application_id, request.into_bytes(), chain_id, None)
            .await?;

        Ok(response)
    }
}
