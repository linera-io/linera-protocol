// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, iter, net::SocketAddr, num::NonZeroU16, sync::Arc};

use async_graphql::{
    futures_util::Stream,
    parser::types::{DocumentOperations, ExecutableDocument, OperationType},
    resolver_utils::ContainerType,
    Error, MergedObject, OutputType, Request, ScalarType, Schema, ServerError, SimpleObject,
    Subscription,
};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{extract::Path, http::StatusCode, response, response::IntoResponse, Extension, Router};
use futures::{lock::Mutex, Future};
use linera_base::{
    crypto::{CryptoError, CryptoHash},
    data_types::{Amount, ApplicationPermissions, Bytecode, TimeDelta, UserApplicationDescription},
    hashed::Hashed,
    identifiers::{ApplicationId, BytecodeId, ChainId, Owner, UserApplicationId},
    ownership::{ChainOwnership, TimeoutConfig},
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
    system::{AdminOperation, Recipient, SystemChannel},
    Operation, Query, Response, SystemOperation,
};
use linera_sdk::base::BlobContent;
use linera_storage::Storage;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error as ThisError;
use tokio::sync::OwnedRwLockReadGuard;
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info, instrument};

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
    #[error("could not decode query string: {0}")]
    QueryStringError(#[from] hex::FromHexError),
    #[error(transparent)]
    BcsError(#[from] bcs::Error),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error("missing GraphQL operation")]
    MissingOperation,
    #[error("unsupported query type: subscription")]
    UnsupportedQueryType,
    #[error("GraphQL operations of different types submitted")]
    HeterogeneousOperations,
    #[error("failed to parse GraphQL query: {error}")]
    GraphQLParseError { error: String },
    #[error("malformed application response")]
    MalformedApplicationResponse,
    #[error("application service error: {errors:?}")]
    ApplicationServiceError { errors: Vec<String> },
    #[error("chain ID not found: {chain_id}")]
    UnknownChainId { chain_id: String },
    #[error("malformed chain ID: {0}")]
    InvalidChainId(CryptoError),
}

impl From<ServerError> for NodeServiceError {
    fn from(value: ServerError) -> Self {
        NodeServiceError::GraphQLParseError {
            error: value.to_string(),
        }
    }
}

impl IntoResponse for NodeServiceError {
    fn into_response(self) -> response::Response {
        let tuple = match self {
            NodeServiceError::BcsHexError(e) => (StatusCode::BAD_REQUEST, vec![e.to_string()]),
            NodeServiceError::QueryStringError(e) => (StatusCode::BAD_REQUEST, vec![e.to_string()]),
            NodeServiceError::ChainClientError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, vec![e.to_string()])
            }
            NodeServiceError::BcsError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, vec![e.to_string()])
            }
            NodeServiceError::JsonError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, vec![e.to_string()])
            }
            NodeServiceError::MalformedApplicationResponse => {
                (StatusCode::INTERNAL_SERVER_ERROR, vec![self.to_string()])
            }
            NodeServiceError::MissingOperation
            | NodeServiceError::HeterogeneousOperations
            | NodeServiceError::UnsupportedQueryType => {
                (StatusCode::BAD_REQUEST, vec![self.to_string()])
            }
            NodeServiceError::GraphQLParseError { error } => (StatusCode::BAD_REQUEST, vec![error]),
            NodeServiceError::ApplicationServiceError { errors } => {
                (StatusCode::BAD_REQUEST, errors)
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
                let operation = Operation::System(system_operation.clone());
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
    /// If no owner is given, try to take the units out of the unattributed account.
    async fn transfer(
        &self,
        chain_id: ChainId,
        owner: Option<Owner>,
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
        owner: Owner,
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
        owner: Owner,
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
        owners: Vec<Owner>,
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
    async fn change_owner(&self, chain_id: ChainId, new_owner: Owner) -> Result<CryptoHash, Error> {
        let operation = SystemOperation::ChangeOwnership {
            super_owners: vec![new_owner],
            owners: Vec::new(),
            multi_leader_rounds: 2,
            timeout_config: TimeoutConfig::default(),
        };
        self.execute_system_operation(operation, chain_id).await
    }

    /// Changes the authentication key of the chain.
    #[expect(clippy::too_many_arguments)]
    async fn change_multiple_owners(
        &self,
        chain_id: ChainId,
        new_owners: Vec<Owner>,
        new_weights: Vec<u64>,
        multi_leader_rounds: u32,
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
    async fn change_application_permissions(
        &self,
        chain_id: ChainId,
        close_chain: Vec<ApplicationId>,
        execute_operations: Option<Vec<ApplicationId>>,
        mandatory_applications: Vec<ApplicationId>,
    ) -> Result<CryptoHash, Error> {
        let operation = SystemOperation::ChangeApplicationPermissions(ApplicationPermissions {
            execute_operations,
            mandatory_applications,
            close_chain,
        });
        self.execute_system_operation(operation, chain_id).await
    }

    /// (admin chain only) Registers a new committee. This will notify the subscribers of
    /// the admin chain so that they can migrate to the new epoch (by accepting the
    /// notification as an "incoming message" in a next block).
    async fn create_committee(
        &self,
        chain_id: ChainId,
        epoch: Epoch,
        committee: Committee,
    ) -> Result<CryptoHash, Error> {
        let operation =
            SystemOperation::Admin(AdminOperation::CreateCommittee { epoch, committee });
        self.execute_system_operation(operation, chain_id).await
    }

    /// Subscribes to a system channel.
    async fn subscribe(
        &self,
        subscriber_chain_id: ChainId,
        publisher_chain_id: ChainId,
        channel: SystemChannel,
    ) -> Result<CryptoHash, Error> {
        let operation = SystemOperation::Subscribe {
            chain_id: publisher_chain_id,
            channel,
        };
        self.execute_system_operation(operation, subscriber_chain_id)
            .await
    }

    /// Unsubscribes from a system channel.
    async fn unsubscribe(
        &self,
        subscriber_chain_id: ChainId,
        publisher_chain_id: ChainId,
        channel: SystemChannel,
    ) -> Result<CryptoHash, Error> {
        let operation = SystemOperation::Unsubscribe {
            chain_id: publisher_chain_id,
            channel,
        };
        self.execute_system_operation(operation, subscriber_chain_id)
            .await
    }

    /// (admin chain only) Removes a committee. Once this message is accepted by a chain,
    /// blocks from the retired epoch will not be accepted until they are followed (hence
    /// re-certified) by a block certified by a recent committee.
    async fn remove_committee(&self, chain_id: ChainId, epoch: Epoch) -> Result<CryptoHash, Error> {
        let operation = SystemOperation::Admin(AdminOperation::RemoveCommittee { epoch });
        self.execute_system_operation(operation, chain_id).await
    }

    /// Publishes a new application bytecode.
    async fn publish_bytecode(
        &self,
        chain_id: ChainId,
        contract: Bytecode,
        service: Bytecode,
    ) -> Result<BytecodeId, Error> {
        self.apply_client_command(&chain_id, move |client| {
            let contract = contract.clone();
            let service = service.clone();
            async move {
                let result = client
                    .publish_bytecode(contract, service)
                    .await
                    .map_err(Error::from)
                    .map(|outcome| outcome.map(|(bytecode_id, _)| bytecode_id));
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
        bytecode_id: BytecodeId,
        parameters: String,
        instantiation_argument: String,
        required_application_ids: Vec<UserApplicationId>,
    ) -> Result<ApplicationId, Error> {
        self.apply_client_command(&chain_id, move |client| {
            let parameters = parameters.as_bytes().to_vec();
            let instantiation_argument = instantiation_argument.as_bytes().to_vec();
            let required_application_ids = required_application_ids.clone();
            async move {
                let result = client
                    .create_application_untyped(
                        bytecode_id,
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

    /// Requests a `RegisterApplications` message from another chain so the application can be used
    /// on this one.
    async fn request_application(
        &self,
        chain_id: ChainId,
        application_id: UserApplicationId,
        target_chain_id: Option<ChainId>,
    ) -> Result<CryptoHash, Error> {
        loop {
            let client = self.context.lock().await.make_chain_client(chain_id)?;
            let result = client
                .request_application(application_id, target_chain_id)
                .await;
            self.context.lock().await.update_wallet(&client).await?;
            let timeout = match result? {
                ClientOutcome::Committed(certificate) => return Ok(certificate.hash()),
                ClientOutcome::WaitForTimeout(timeout) => timeout,
            };
            let mut stream = client.subscribe().await?;
            drop(client);
            util::wait_for_next_round(&mut stream, timeout).await;
        }
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
    id: UserApplicationId,
    description: UserApplicationDescription,
    link: String,
}

impl ApplicationOverview {
    fn new(
        id: UserApplicationId,
        description: UserApplicationDescription,
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

/// Given a parsed GraphQL query (or `ExecutableDocument`), returns the `OperationType`.
///
/// Errors:
///
/// If we have no `OperationType`s or the `OperationTypes` are heterogeneous, i.e. a query
/// was submitted with a `mutation` and `subscription`.
fn operation_type(document: &ExecutableDocument) -> Result<OperationType, NodeServiceError> {
    match &document.operations {
        DocumentOperations::Single(op) => Ok(op.node.ty),
        DocumentOperations::Multiple(ops) => {
            let mut op_types = ops.values().map(|v| v.node.ty);
            let first = op_types.next().ok_or(NodeServiceError::MissingOperation)?;
            op_types
                .all(|x| x == first)
                .then_some(first)
                .ok_or(NodeServiceError::HeterogeneousOperations)
        }
    }
}

/// Extracts the underlying byte vector from a serialized GraphQL response
/// from an application.
fn bytes_from_response(data: async_graphql::Value) -> Vec<Vec<u8>> {
    if let async_graphql::Value::Object(map) = data {
        map.values()
            .filter_map(|value| {
                if let async_graphql::Value::List(list) = value {
                    bytes_from_list(list)
                } else {
                    None
                }
            })
            .collect()
    } else {
        vec![]
    }
}

fn bytes_from_list(list: &[async_graphql::Value]) -> Option<Vec<u8>> {
    list.iter()
        .map(|item| {
            if let async_graphql::Value::Number(n) = item {
                n.as_u64().map(|n| n as u8)
            } else {
                None
            }
        })
        .collect()
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

    /// Handles queries for user applications.
    async fn user_application_query(
        &self,
        application_id: UserApplicationId,
        request: &Request,
        chain_id: ChainId,
    ) -> Result<async_graphql::Response, NodeServiceError> {
        let bytes = serde_json::to_vec(&request)?;
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
        let response = client.query_application(query).await?;
        let user_response_bytes = match response {
            Response::System(_) => unreachable!("cannot get a system response for a user query"),
            Response::User(user) => user,
        };
        Ok(serde_json::from_slice(&user_response_bytes)?)
    }

    /// Handles mutations for user applications.
    async fn user_application_mutation(
        &self,
        application_id: UserApplicationId,
        request: &Request,
        chain_id: ChainId,
    ) -> Result<async_graphql::Response, NodeServiceError> {
        debug!("Request: {:?}", &request);
        let graphql_response = self
            .user_application_query(application_id, request, chain_id)
            .await?;
        if graphql_response.is_err() {
            let errors = graphql_response
                .errors
                .iter()
                .map(|e| e.to_string())
                .collect();
            return Err(NodeServiceError::ApplicationServiceError { errors });
        }
        debug!("Response: {:?}", &graphql_response);
        let bcs_bytes_list = bytes_from_response(graphql_response.data);
        if bcs_bytes_list.is_empty() {
            return Err(NodeServiceError::MalformedApplicationResponse);
        }
        let operations = bcs_bytes_list
            .into_iter()
            .map(|bytes| Operation::User {
                application_id,
                bytes,
            })
            .collect::<Vec<_>>();

        let client = self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .map_err(|_| NodeServiceError::UnknownChainId {
                chain_id: chain_id.to_string(),
            })?;
        let hash = loop {
            let timeout = match client.execute_operations(operations.clone()).await? {
                ClientOutcome::Committed(certificate) => break certificate.hash(),
                ClientOutcome::WaitForTimeout(timeout) => timeout,
            };
            let mut stream = client.subscribe().await.map_err(|_| {
                ChainClientError::InternalError("Could not subscribe to the local node.")
            })?;
            util::wait_for_next_round(&mut stream, timeout).await;
        };
        Ok(async_graphql::Response::new(hash.to_value()))
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
        request: GraphQLRequest,
    ) -> Result<GraphQLResponse, NodeServiceError> {
        let mut request = request.into_inner();

        let parsed_query = request.parsed_query()?;
        let operation_type = operation_type(parsed_query)?;

        let chain_id: ChainId = chain_id.parse().map_err(NodeServiceError::InvalidChainId)?;
        let application_id: UserApplicationId = application_id.parse()?;

        let response = match operation_type {
            OperationType::Query => {
                service
                    .0
                    .user_application_query(application_id, &request, chain_id)
                    .await?
            }
            OperationType::Mutation => {
                service
                    .0
                    .user_application_mutation(application_id, &request, chain_id)
                    .await?
            }
            OperationType::Subscription => return Err(NodeServiceError::UnsupportedQueryType),
        };

        Ok(response.into())
    }
}
