// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The server component of the Linera faucet.

use std::{net::SocketAddr, num::NonZeroU16, sync::Arc};

use async_graphql::{EmptySubscription, Error, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{Extension, Router};
use futures::lock::Mutex;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{Amount, ApplicationPermissions, Timestamp},
    identifiers::{AccountOwner, ChainId, MessageId},
    ownership::ChainOwnership,
};
use linera_client::{
    chain_listener::{ChainListener, ChainListenerConfig, ClientContext},
    config::GenesisConfig,
};
use linera_core::data_types::ClientOutcome;
use linera_storage::{Clock as _, Storage};
use serde::Deserialize;
use tower_http::cors::CorsLayer;
use tracing::info;

/// A rough estimate of the maximum fee for a block.
const MAX_FEE: Amount = Amount::from_millis(100);

/// Returns an HTML response constructing the GraphiQL web page for the given URI.
pub(crate) async fn graphiql(uri: axum::http::Uri) -> impl axum::response::IntoResponse {
    axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(uri.path())
            .subscription_endpoint("/ws")
            .finish(),
    )
}

#[cfg(test)]
mod tests;

/// The root GraphQL query type.
pub struct QueryRoot<C> {
    context: Arc<Mutex<C>>,
    genesis_config: Arc<GenesisConfig>,
    chain_id: ChainId,
}

/// The root GraphQL mutation type.
pub struct MutationRoot<C> {
    /// The chain from which new faucet chains are opened.
    main_chain_id: ChainId,
    /// The chain that is currently used to open requested chains for clients.
    faucet_chain_id: Arc<Mutex<Option<ChainId>>>,
    context: Arc<Mutex<C>>,
    amount: Amount,
    faucet_init_balance: Amount,
    end_timestamp: Timestamp,
    start_timestamp: Timestamp,
    start_balance: Amount,
}

/// The result of a successful `claim` mutation.
#[derive(SimpleObject)]
pub struct ClaimOutcome {
    /// The ID of the message that created the new chain.
    pub message_id: MessageId,
    /// The ID of the new chain.
    pub chain_id: ChainId,
    /// The hash of the parent chain's certificate containing the `OpenChain` operation.
    pub certificate_hash: CryptoHash,
}

#[derive(Debug, Deserialize, SimpleObject)]
pub struct Validator {
    pub public_key: ValidatorPublicKey,
    pub network_address: String,
}

#[async_graphql::Object(cache_control(no_cache))]
impl<C> QueryRoot<C>
where
    C: ClientContext,
{
    /// Returns the version information on this faucet service.
    async fn version(&self) -> linera_version::VersionInfo {
        linera_version::VersionInfo::default()
    }

    /// Returns the genesis config.
    async fn genesis_config(&self) -> Result<serde_json::Value, Error> {
        Ok(serde_json::to_value(&*self.genesis_config)?)
    }

    /// Returns the current committee's validators.
    async fn current_validators(&self) -> Result<Vec<Validator>, Error> {
        let client = self.context.lock().await.make_chain_client(self.chain_id)?;
        let committee = client.local_committee().await?;
        Ok(committee
            .validators()
            .iter()
            .map(|(public_key, validator)| Validator {
                public_key: *public_key,
                network_address: validator.network_address.clone(),
            })
            .collect())
    }
}

#[async_graphql::Object(cache_control(no_cache))]
impl<C> MutationRoot<C>
where
    C: ClientContext,
{
    /// Creates a new chain with the given authentication key, and transfers tokens to it.
    async fn claim(&self, owner: AccountOwner) -> Result<ClaimOutcome, Error> {
        self.do_claim(owner).await
    }
}

impl<C> MutationRoot<C>
where
    C: ClientContext,
{
    async fn do_claim(&self, owner: AccountOwner) -> Result<ClaimOutcome, Error> {
        let maybe_faucet_chain_id = *self.faucet_chain_id.lock().await;
        let faucet_chain_id = match maybe_faucet_chain_id {
            Some(faucet_chain_id) => faucet_chain_id,
            None => self.open_new_faucet_chain().await?,
        };
        let (faucet_client, main_client) = {
            let guard = self.context.lock().await;
            (
                guard.make_chain_client(faucet_chain_id)?,
                guard.make_chain_client(self.main_chain_id)?,
            )
        };

        if self.start_timestamp < self.end_timestamp {
            let local_time = faucet_client.storage_client().clock().current_time();
            if local_time < self.end_timestamp {
                let full_duration = self
                    .end_timestamp
                    .delta_since(self.start_timestamp)
                    .as_micros();
                let remaining_duration = self.end_timestamp.delta_since(local_time).as_micros();
                let balance = faucet_client
                    .local_balance()
                    .await?
                    .try_add(main_client.local_balance().await?)?;
                let Ok(remaining_balance) = balance.try_sub(self.amount) else {
                    return Err(Error::new("The faucet is empty."));
                };
                // The tokens unlock linearly, e.g. if 1/3 of the time is left, then 1/3 of the
                // tokens remain locked, so the remaining balance must be at least 1/3 of the start
                // balance. In general:
                // start_balance / full_duration <= remaining_balance / remaining_duration.
                if Self::multiply(u128::from(self.start_balance), remaining_duration)
                    > Self::multiply(u128::from(remaining_balance), full_duration)
                {
                    return Err(Error::new("Not enough unlocked balance; try again later."));
                }
            }
        }

        let ownership = ChainOwnership::single(owner);
        let (message_id, certificate) = faucet_client
            .open_chain(ownership, ApplicationPermissions::default(), self.amount)
            .await?
            .try_unwrap()?;
        self.context
            .lock()
            .await
            .update_wallet(&faucet_client)
            .await?;

        // Only keep using this chain if there will still be enough balance to close it.
        if faucet_client.local_balance().await? < self.amount.try_add(MAX_FEE.try_mul(2)?)? {
            self.faucet_chain_id.lock().await.take();
            // TODO(#1795): Move the remaining tokens back to the main chain.
            match faucet_client.close_chain().await {
                Ok(outcome) => {
                    outcome.try_unwrap()?;
                }
                Err(err) => tracing::warn!("Failed to close the temporary faucet chain: {err:?}"),
            }
            if let Err(err) = self
                .context
                .lock()
                .await
                .forget_chain(&faucet_chain_id)
                .await
            {
                tracing::error!(
                    "Failed to remove the temporary faucet chain from the wallet: {err:?}"
                );
            }
        }

        let chain_id = ChainId::child(message_id);
        Ok(ClaimOutcome {
            message_id,
            chain_id,
            certificate_hash: certificate.hash(),
        })
    }

    async fn open_new_faucet_chain(&self) -> Result<ChainId, Error> {
        let main_client = self
            .context
            .lock()
            .await
            .make_chain_client(self.main_chain_id)?;
        let main_balance = main_client.local_balance().await?;
        let key_pair = main_client.key_pair().await?;
        let balance = self.faucet_init_balance.min(main_balance.try_sub(MAX_FEE)?);
        let ownership = main_client.chain_state_view().await?.ownership().clone();
        let (message_id, certificate) = main_client
            .open_chain(ownership, ApplicationPermissions::default(), balance)
            .await?
            .try_unwrap()?;
        let chain_id = ChainId::child(message_id);
        info!("Switching to a new faucet chain {chain_id:8}");
        self.context
            .lock()
            .await
            .update_wallet_for_new_chain(
                chain_id,
                Some(key_pair),
                certificate.block().header.timestamp,
            )
            .await?;
        *self.faucet_chain_id.lock().await = Some(chain_id);
        Ok(chain_id)
    }
}

impl<C> MutationRoot<C> {
    /// Multiplies a `u128` with a `u64` and returns the result as a 192-bit number.
    fn multiply(a: u128, b: u64) -> [u64; 3] {
        let lower = u128::from(u64::MAX);
        let b = u128::from(b);
        let mut a1 = (a >> 64) * b;
        let a0 = (a & lower) * b;
        a1 += a0 >> 64;
        [(a1 >> 64) as u64, (a1 & lower) as u64, (a0 & lower) as u64]
    }
}

/// A GraphQL interface to request a new chain with tokens.
pub struct FaucetService<C>
where
    C: ClientContext,
{
    main_chain_id: ChainId,
    faucet_chain_id: Arc<Mutex<Option<ChainId>>>,
    context: Arc<Mutex<C>>,
    genesis_config: Arc<GenesisConfig>,
    config: ChainListenerConfig,
    storage: C::Storage,
    port: NonZeroU16,
    amount: Amount,
    end_timestamp: Timestamp,
    faucet_init_balance: Amount,
    start_timestamp: Timestamp,
    start_balance: Amount,
}

impl<C> Clone for FaucetService<C>
where
    C: ClientContext,
{
    fn clone(&self) -> Self {
        Self {
            main_chain_id: self.main_chain_id,
            faucet_chain_id: Arc::clone(&self.faucet_chain_id),
            context: Arc::clone(&self.context),
            genesis_config: Arc::clone(&self.genesis_config),
            config: self.config.clone(),
            storage: self.storage.clone(),
            port: self.port,
            amount: self.amount,
            faucet_init_balance: self.faucet_init_balance,
            end_timestamp: self.end_timestamp,
            start_timestamp: self.start_timestamp,
            start_balance: self.start_balance,
        }
    }
}

impl<C> FaucetService<C>
where
    C: ClientContext,
{
    /// Creates a new instance of the faucet service.
    #[expect(clippy::too_many_arguments)]
    pub async fn new(
        port: NonZeroU16,
        chain_id: ChainId,
        context: C,
        amount: Amount,
        max_claims_per_chain: u32,
        end_timestamp: Timestamp,
        genesis_config: Arc<GenesisConfig>,
        config: ChainListenerConfig,
        storage: C::Storage,
    ) -> anyhow::Result<Self> {
        let faucet_init_balance = amount
            .try_add(MAX_FEE)?
            .try_mul(u128::from(max_claims_per_chain))?
            .try_add(MAX_FEE)?; // One more block fee for closing the chain.
        let client = context.make_chain_client(chain_id)?;
        let context = Arc::new(Mutex::new(context));
        let start_timestamp = client.storage_client().clock().current_time();
        client.process_inbox().await?;
        let start_balance = client.local_balance().await?;
        Ok(Self {
            main_chain_id: chain_id,
            faucet_chain_id: Arc::new(Mutex::new(None)),
            context,
            genesis_config,
            config,
            storage,
            port,
            amount,
            faucet_init_balance,
            end_timestamp,
            start_timestamp,
            start_balance,
        })
    }

    pub fn schema(&self) -> Schema<QueryRoot<C>, MutationRoot<C>, EmptySubscription> {
        let mutation_root = MutationRoot {
            main_chain_id: self.main_chain_id,
            faucet_chain_id: Arc::clone(&self.faucet_chain_id),
            context: Arc::clone(&self.context),
            amount: self.amount,
            faucet_init_balance: self.faucet_init_balance,
            end_timestamp: self.end_timestamp,
            start_timestamp: self.start_timestamp,
            start_balance: self.start_balance,
        };
        let query_root = QueryRoot {
            genesis_config: Arc::clone(&self.genesis_config),
            context: Arc::clone(&self.context),
            chain_id: self.main_chain_id,
        };
        Schema::build(query_root, mutation_root, EmptySubscription).finish()
    }

    /// Runs the faucet.
    #[tracing::instrument(
        name = "FaucetService::run",
        skip_all, fields(port = self.port, chain_id = ?self.main_chain_id))
    ]
    pub async fn run(self) -> anyhow::Result<()> {
        let port = self.port.get();
        let index_handler = axum::routing::get(graphiql).post(Self::index_handler);

        let app = Router::new()
            .route("/", index_handler)
            .route("/ready", axum::routing::get(|| async { "ready!" }))
            .route_service("/ws", GraphQLSubscription::new(self.schema()))
            .layer(Extension(self.clone()))
            .layer(CorsLayer::permissive());

        info!("GraphiQL IDE: http://localhost:{}", port);

        let context = Arc::clone(&self.context);
        let listener = ChainListener::new(self.config.clone(), context, self.storage.clone());
        listener.run().await;

        axum::serve(
            tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?,
            app,
        )
        .await?;

        Ok(())
    }

    /// Executes a GraphQL query and generates a response for our `Schema`.
    async fn index_handler(service: Extension<Self>, request: GraphQLRequest) -> GraphQLResponse {
        let schema = service.0.schema();
        schema.execute(request.into_inner()).await.into()
    }
}

trait ClientOutcomeExt {
    type Output;

    /// Returns the committed result or an error if we are not the leader.
    ///
    /// It is recommended to use single-owner chains for the faucet to avoid this error.
    fn try_unwrap(self) -> Result<Self::Output, Error>;
}

impl<T> ClientOutcomeExt for ClientOutcome<T> {
    type Output = T;

    fn try_unwrap(self) -> Result<Self::Output, Error> {
        match self {
            ClientOutcome::Committed(result) => Ok(result),
            ClientOutcome::WaitForTimeout(timeout) => Err(Error::new(format!(
                "This faucet is using a multi-owner chain and is not the leader right now. \
                Try again at {}",
                timeout.timestamp,
            ))),
        }
    }
}
