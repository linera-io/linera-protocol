// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, iter, net::SocketAddr, num::NonZeroU16, sync::Arc};

use async_graphql::{
    futures_util::Stream, resolver_utils::ContainerType, Error, MergedObject, OutputType,
    ScalarType, Schema, SimpleObject, Subscription,
};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{extract::Path, http::StatusCode, response, response::IntoResponse, Extension, Router};
use futures::{lock::Mutex, Future};
use linera_base::{
    crypto::{CryptoError, CryptoHash},
    data_types::{Amount, ApplicationDescription, ApplicationPermissions, Bytecode, TimeDelta},
    hashed::Hashed,
    identifiers::{AccountOwner, ApplicationId, ChainId, ModuleId},
    ownership::{ChainOwnership, TimeoutConfig},
    vm::VmRuntime,
    BcsHexParseError,
};
use linera_chain::{
    types::{ConfirmedBlock, GenericCertificate},
    ChainStateView,
};
use linera_client::chain_listener::{ChainListener, ChainListenerConfig, ClientContext};
use linera_core::{
    client::{ChainClient, ChainClientError},
    data_types::ClientOutcome,
    worker::Notification,
};
use linera_execution::{
    committee::{Committee, Epoch},
    system::{AdminOperation, Recipient},
    Operation, Query, QueryOutcome, QueryResponse, SystemOperation,
};
use linera_sdk::linera_base_types::BlobContent;
use linera_storage::Storage;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error as ThisError;
use tokio::sync::OwnedRwLockReadGuard;
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info, instrument, trace};

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

#[derive(Debug, ThisError)]
enum NodeServiceError {
    #[error(transparent)]
    ChainClientError(#[from] ChainClientError),
    #[error(transparent)]
    BcsHexError(#[from] BcsHexParseError),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error("chain ID not found: {chain_id}")]
    UnknownChainId { chain_id: String },
    #[error("malformed chain ID: {0}")]
    InvalidChainId(CryptoError),
}

impl IntoResponse for NodeServiceError {
    fn into_response(self) -> response::Response {
        let tuple = match self {
            NodeServiceError::BcsHexError(e) => (StatusCode::BAD_REQUEST, vec![e.to_string()]),
            NodeServiceError::ChainClientError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, vec![e.to_string()])
            }
            NodeServiceError::JsonError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, vec![e.to_string()])
            }
            NodeServiceError::UnknownChainId { chain_id } => (
                StatusCode::NOT_FOUND,
                vec![format!("unknown chain ID: {}", chain_id)],
            ),
            NodeServiceError::InvalidChainId(_) => (
                StatusCode::BAD_REQUEST,
                vec!["invalid chain ID".to_string()],
            ),
        };
        let tuple = (tuple.0, json!({"error": tuple.1}).to_string());
        tuple.into_response()
    }
}

#[Subscription]
impl<C> SubscriptionRoot<C>
where
    C: ClientContext,
{
    /// Subscribes to notifications from the specified chain.
    async fn notifications(
        &self,
        chain_id: ChainId,
    ) -> Result<impl Stream<Item = Notification>, Error> {
        let client = self.context.lock().await.make_chain_client(chain_id)?;
        Ok(client.subscribe().await?)
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
        F: FnMut(ChainClient<C::ValidatorNodeProvider, C::Storage>) -> Fut,
        Fut: Future<
            Output = (
                Result<ClientOutcome<T>, Error>,
                ChainClient<C::ValidatorNodeProvider, C::Storage>,
            ),
        >,
    {
        loop {
            let client = self.context.lock().await.make_chain_client(*chain_id)?;
            let mut stream = client.subscribe().await?;
            let (result, client) = f(client).await;
            self.context.lock().await.update_wallet(&client).await?;
            let timeout = match result? {
                ClientOutcome::Committed(t) => return Ok(t),
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
    C: ClientContext,
{
    /// Processes the inbox and returns the lists of certificate hashes that were created, if any.
    async fn process_inbox(&self, chain_id: ChainId) -> Result<Vec<CryptoHash>, Error> {
        let mut hashes = Vec::new();
        loop {
            let client = self.context.lock().await.make_chain_client(chain_id)?;
            client.synchronize_from_validators().await?;
            let result = client.process_inbox_without_prepare().await;
            self.context.lock().await.update_wallet(&client).await?;
            let (certificates, maybe_timeout) = result?;
            hashes.extend(certificates.into_iter().map(|cert| cert.hash()));
            match maybe_timeout {
                None => return Ok(hashes),
                Some(timestamp) => {
                    let mut stream = client.subscribe().await?;
                    drop(client);
                    util::wait_for_next_round(&mut stream, timestamp).await;
                }
            }
        }
    }

    /// Retries the pending block that was unsuccessfully proposed earlier.
    async fn retry_pending_block(&self, chain_id: ChainId) -> Result<Option<CryptoHash>, Error> {
        let client = self.context.lock().await.make_chain_client(chain_id)?;
        let outcome = client.process_pending_block().await?;
        self.context.lock().await.update_wallet(&client).await?;
        match outcome {
            ClientOutcome::Committed(Some(certificate)) => Ok(Some(certificate.hash())),
            ClientOutcome::Committed(None) => Ok(None),
            ClientOutcome::WaitForTimeout(timeout) => Err(Error::from(format!(
                "Please try again at {}",
                timeout.timestamp
            ))),
        }
    }

    /// Transfers `amount` units of value from the given owner's account to the recipient.
    /// If no owner is given, try to take the units out of the chain account.
    async fn transfer(
        &self,
        chain_id: ChainId,
        owner: AccountOwner,
        recipient: Recipient,
        amount: Amount,
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
        chain_id: ChainId,
        owner: AccountOwner,
        target_id: ChainId,
        recipient: Recipient,
        amount: Amount,
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
    #[allow(clippy::too_many_arguments)]
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

    /// Creates (or activates) a new chain with the given owner.
    /// This will automatically subscribe to the future committees created by `admin_id`.
    async fn open_chain(
        &self,
        chain_id: ChainId,
        owner: AccountOwner,
        balance: Option<Amount>,
    ) -> Result<ChainId, Error> {
        let ownership = ChainOwnership::single(owner);
        let balance = balance.unwrap_or(Amount::ZERO);
        let message_id = self
            .apply_client_command(&chain_id, move |client| {
                let ownership = ownership.clone();
                async move {
                    let result = client
                        .open_chain(ownership, ApplicationPermissions::default(), balance)
                        .await
                        .map_err(Error::from)
                        .map(|outcome| outcome.map(|(message_id, _)| message_id));
                    (result, client)
                }
            })
            .await?;
        Ok(ChainId::child(message_id))
    }

    /// Creates (or activates) a new chain by installing the given authentication keys.
    /// This will automatically subscribe to the future committees created by `admin_id`.
    #[expect(clippy::too_many_arguments)]
    async fn open_multi_owner_chain(
        &self,
        chain_id: ChainId,
        application_permissions: Option<ApplicationPermissions>,
        owners: Vec<AccountOwner>,
        weights: Option<Vec<u64>>,
        multi_leader_rounds: Option<u32>,
        balance: Option<Amount>,
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
        let message_id = self
            .apply_client_command(&chain_id, move |client| {
                let ownership = ownership.clone();
                let application_permissions = application_permissions.clone().unwrap_or_default();
                async move {
                    let result = client
                        .open_chain(ownership, application_permissions, balance)
                        .await
                        .map_err(Error::from)
                        .map(|outcome| outcome.map(|(message_id, _)| message_id));
                    (result, client)
                }
            })
            .await?;
        Ok(ChainId::child(message_id))
    }

    /// Closes the chain. Returns `None` if it was already closed.
    async fn close_chain(&self, chain_id: ChainId) -> Result<Option<CryptoHash>, Error> {
        let maybe_cert = self
            .apply_client_command(&chain_id, |client| async move {
                let result = client.close_chain().await.map_err(Error::from);
                (result, client)
            })
            .await?;
        Ok(maybe_cert.as_ref().map(GenericCertificate::hash))
    }

    /// Changes the authentication key of the chain.
    async fn change_owner(
        &self,
        chain_id: ChainId,
        new_owner: AccountOwner,
    ) -> Result<CryptoHash, Error> {
        let operation = SystemOperation::ChangeOwnership {
            super_owners: vec![new_owner],
            owners: Vec::new(),
            multi_leader_rounds: 2,
            open_multi_leader_rounds: false,
            timeout_config: TimeoutConfig::default(),
        };
        self.execute_system_operation(operation, chain_id).await
    }

    /// Changes the authentication key of the chain.
    #[expect(clippy::too_many_arguments)]
    async fn change_multiple_owners(
        &self,
        chain_id: ChainId,
        new_owners: Vec<AccountOwner>,
        new_weights: Vec<u64>,
        multi_leader_rounds: u32,
        open_multi_leader_rounds: bool,
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
        chain_id: ChainId,
        close_chain: Vec<ApplicationId>,
        execute_operations: Option<Vec<ApplicationId>>,
        mandatory_applications: Vec<ApplicationId>,
        change_application_permissions: Vec<ApplicationId>,
        call_service_as_oracle: Option<Vec<ApplicationId>>,
        make_http_requests: Option<Vec<ApplicationId>>,
    ) -> Result<CryptoHash, Error> {
        let operation = SystemOperation::ChangeApplicationPermissions(ApplicationPermissions {
            execute_operations,
            mandatory_applications,
            close_chain,
            change_application_permissions,
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
        chain_id: ChainId,
        contract: Bytecode,
        service: Bytecode,
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
        chain_id: ChainId,
        bytes: Vec<u8>,
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
        chain_id: ChainId,
        module_id: ModuleId,
        parameters: String,
        instantiation_argument: String,
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
    C: ClientContext,
{
    async fn chain(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainStateExtendedView<<C::Storage as Storage>::Context>, Error> {
        let client = self.context.lock().await.make_chain_client(chain_id)?;
        let view = client.chain_state_view().await?;
        Ok(ChainStateExtendedView::new(view))
    }

    async fn applications(&self, chain_id: ChainId) -> Result<Vec<ApplicationOverview>, Error> {
        let client = self.context.lock().await.make_chain_client(chain_id)?;
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
            list: self.context.lock().await.wallet().chain_ids(),
            default: self.default_chain,
        })
    }

    async fn block(
        &self,
        hash: Option<CryptoHash>,
        chain_id: ChainId,
    ) -> Result<Option<Hashed<ConfirmedBlock>>, Error> {
        let client = self.context.lock().await.make_chain_client(chain_id)?;
        let hash = match hash {
            Some(hash) => Some(hash),
            None => {
                let view = client.chain_state_view().await?;
                view.tip_state.get().block_hash
            }
        };
        if let Some(hash) = hash {
            let block = client.read_hashed_confirmed_block(hash).await?;
            Ok(Some(block))
        } else {
            Ok(None)
        }
    }

    async fn blocks(
        &self,
        from: Option<CryptoHash>,
        chain_id: ChainId,
        limit: Option<u32>,
    ) -> Result<Vec<Hashed<ConfirmedBlock>>, Error> {
        let client = self.context.lock().await.make_chain_client(chain_id)?;
        let limit = limit.unwrap_or(10);
        let from = match from {
            Some(from) => Some(from),
            None => {
                let view = client.chain_state_view().await?;
                view.tip_state.get().block_hash
            }
        };
        if let Some(from) = from {
            let values = client
                .read_hashed_confirmed_blocks_downward(from, limit)
                .await?;
            Ok(values)
        } else {
            Ok(vec![])
        }
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

/// The `NodeService` is a server that exposes a web-server to the client.
/// The node service is primarily used to explore the state of a chain in GraphQL.
pub struct NodeService<C>
where
    C: ClientContext,
{
    config: ChainListenerConfig,
    port: NonZeroU16,
    default_chain: Option<ChainId>,
    storage: C::Storage,
    context: Arc<Mutex<C>>,
}

impl<C> Clone for NodeService<C>
where
    C: ClientContext,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            port: self.port,
            default_chain: self.default_chain,
            storage: self.storage.clone(),
            context: Arc::clone(&self.context),
        }
    }
}

impl<C> NodeService<C>
where
    C: ClientContext,
{
    /// Creates a new instance of the node service given a client chain and a port.
    pub async fn new(
        config: ChainListenerConfig,
        port: NonZeroU16,
        default_chain: Option<ChainId>,
        storage: C::Storage,
        context: C,
    ) -> Self {
        Self {
            config,
            port,
            default_chain,
            storage,
            context: Arc::new(Mutex::new(context)),
        }
    }

    pub fn schema(&self) -> Schema<QueryRoot<C>, MutationRoot<C>, SubscriptionRoot<C>> {
        Schema::build(
            QueryRoot {
                context: Arc::clone(&self.context),
                port: self.port,
                default_chain: self.default_chain,
            },
            MutationRoot {
                context: Arc::clone(&self.context),
            },
            SubscriptionRoot {
                context: Arc::clone(&self.context),
            },
        )
        .finish()
    }

    /// Runs the node service.
    #[instrument(name = "node_service", level = "info", skip(self), fields(port = ?self.port))]
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let port = self.port.get();
        let index_handler = axum::routing::get(util::graphiql).post(Self::index_handler);
        let application_handler =
            axum::routing::get(util::graphiql).post(Self::application_handler);

        let app = Router::new()
            .route("/", index_handler)
            .route(
                "/chains/:chain_id/applications/:application_id",
                application_handler,
            )
            .route("/ready", axum::routing::get(|| async { "ready!" }))
            .route_service("/ws", GraphQLSubscription::new(self.schema()))
            .layer(Extension(self.clone()))
            // TODO(#551): Provide application authentication.
            .layer(CorsLayer::permissive());

        info!("GraphiQL IDE: http://localhost:{}", port);

        ChainListener::new(self.config)
            .run(Arc::clone(&self.context), self.storage.clone())
            .await;
        let serve_fut = axum::serve(
            tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).await?,
            app,
        );
        serve_fut.await?;

        Ok(())
    }

    /// Handles service queries for user applications (including mutations).
    async fn handle_service_request(
        &self,
        application_id: ApplicationId,
        request: Vec<u8>,
        chain_id: ChainId,
    ) -> Result<Vec<u8>, NodeServiceError> {
        let QueryOutcome {
            response,
            operations,
        } = self
            .query_user_application(application_id, request, chain_id)
            .await?;
        if operations.is_empty() {
            return Ok(response);
        }

        trace!("Query requested a new block with operations: {operations:?}");
        let client = self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .map_err(|_| NodeServiceError::UnknownChainId {
                chain_id: chain_id.to_string(),
            })?;
        let hash = loop {
            let timeout = match client
                .execute_operations(operations.clone(), vec![])
                .await?
            {
                ClientOutcome::Committed(certificate) => break certificate.hash(),
                ClientOutcome::WaitForTimeout(timeout) => timeout,
            };
            let mut stream = client.subscribe().await.map_err(|_| {
                ChainClientError::InternalError("Could not subscribe to the local node.")
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
            .map_err(|_| NodeServiceError::UnknownChainId {
                chain_id: chain_id.to_string(),
            })?;
        let QueryOutcome {
            response,
            operations,
        } = client.query_application(query).await?;
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
            "Processing request for application {application_id} on chain {chain_id}:\n{:?}",
            &request
        );
        let response = service
            .0
            .handle_service_request(application_id, request.into_bytes(), chain_id)
            .await?;

        Ok(response)
    }
}
