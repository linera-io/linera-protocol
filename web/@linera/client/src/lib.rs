// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
# `linera-web`

This module defines the JavaScript bindings to the client API.

It is compiled to Wasm, with a JavaScript wrapper to inject its imports, and published on
NPM as `@linera/client`.

There is a supplementary package `@linera/signer`, contained within the `signer`
subdirectory, that defines signer implementations for different transaction-signing
policies, including in-memory keys and signing using an existing MetaMask wallet.
*/

// We sometimes need functions in this module to be async in order to
// ensure the generated code will return a `Promise`.
#![allow(clippy::unused_async)]
#![recursion_limit = "256"]

pub mod signer;

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use futures::{future::FutureExt as _, lock::Mutex as AsyncMutex, stream::StreamExt};
use linera_base::identifiers::{AccountOwner, ApplicationId, ChainId};
use linera_client::{
    chain_listener::{ChainListener, ChainListenerConfig, ClientContext as _},
    client_options::ClientContextOptions,
    config::GenesisConfig,
};
use linera_core::{
    data_types::ClientOutcome,
    node::{ValidatorNode as _, ValidatorNodeProvider as _},
    wallet,
};
use linera_views::store::WithError;
use serde::ser::Serialize as _;
use wasm_bindgen::prelude::*;
use web_sys::{js_sys, wasm_bindgen};

use crate::signer::JsSigner;

// TODO(#12): convert to IndexedDbStore once we refactor Context
type WebStorage =
    linera_storage::DbStorage<linera_views::memory::MemoryDatabase, linera_storage::WallClock>;

type WebEnvironment =
    linera_core::environment::Impl<WebStorage, linera_rpc::node_provider::NodeProvider, JsSigner>;

type JsResult<T> = Result<T, JsError>;

async fn get_storage(
) -> Result<WebStorage, <linera_views::memory::MemoryDatabase as WithError>::Error> {
    linera_storage::DbStorage::maybe_create_and_connect(
        &linera_views::memory::MemoryStoreConfig {
            max_stream_queries: 1,
            kill_on_drop: false,
        },
        "linera",
        Some(linera_execution::WasmRuntime::Wasmer),
    )
    .await
}

/// A wallet that stores the user's chains and keys in memory.
#[wasm_bindgen]
pub struct Wallet {
    chains: wallet::Memory,
    default: Option<ChainId>,
    genesis_config: GenesisConfig,
}

type ClientContext = linera_client::client_context::ClientContext<WebEnvironment>;
type ChainClient = linera_core::client::ChainClient<WebEnvironment>;

// TODO(#13): get from user
pub const OPTIONS: ClientContextOptions = ClientContextOptions {
    send_timeout: linera_base::time::Duration::from_millis(4000),
    recv_timeout: linera_base::time::Duration::from_millis(4000),
    max_pending_message_bundles: 10,
    retry_delay: linera_base::time::Duration::from_millis(1000),
    max_retries: 10,
    wait_for_outgoing_messages: false,
    blanket_message_policy: linera_core::client::BlanketMessagePolicy::Accept,
    restrict_chain_ids_to: None,
    reject_message_bundles_without_application_ids: None,
    reject_message_bundles_with_other_application_ids: None,
    long_lived_services: false,
    blob_download_timeout: linera_base::time::Duration::from_millis(1000),
    certificate_batch_download_timeout: linera_base::time::Duration::from_millis(1000),
    certificate_download_batch_size: linera_core::client::DEFAULT_CERTIFICATE_DOWNLOAD_BATCH_SIZE,
    sender_certificate_download_batch_size:
        linera_core::client::DEFAULT_SENDER_CERTIFICATE_DOWNLOAD_BATCH_SIZE,
    chain_worker_ttl: Duration::from_secs(30),
    sender_chain_worker_ttl: Duration::from_millis(200),
    quorum_grace_period: linera_core::DEFAULT_QUORUM_GRACE_PERIOD,
    max_joined_tasks: 100,
    max_accepted_latency_ms: linera_core::client::requests_scheduler::MAX_ACCEPTED_LATENCY_MS,
    cache_ttl_ms: linera_core::client::requests_scheduler::CACHE_TTL_MS,
    cache_max_size: linera_core::client::requests_scheduler::CACHE_MAX_SIZE,
    max_request_ttl_ms: linera_core::client::requests_scheduler::MAX_REQUEST_TTL_MS,
    alpha: linera_core::client::requests_scheduler::ALPHA_SMOOTHING_FACTOR,
    alternative_peers_retry_delay_ms: linera_core::client::requests_scheduler::STAGGERED_DELAY_MS,

    // TODO(linera-protocol#2944): separate these out from the
    // `ClientOptions` struct, since they apply only to the CLI/native
    // client
    wallet_state_path: None,
    keystore_path: None,
    with_wallet: None,
    chrome_trace_exporter: false,
    chrome_trace_file: None,
    otlp_exporter_endpoint: None,
};

const BLOCK_CACHE_SIZE: usize = 5000;
const EXECUTION_STATE_CACHE_SIZE: usize = 10000;

#[wasm_bindgen]
pub struct Faucet(linera_faucet_client::Faucet);

#[wasm_bindgen]
impl Faucet {
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new(url: String) -> Faucet {
        Faucet(linera_faucet_client::Faucet::new(url))
    }

    /// Creates a new wallet from the faucet.
    ///
    /// # Errors
    /// If we couldn't retrieve the genesis config from the faucet.
    #[wasm_bindgen(js_name = createWallet)]
    pub async fn create_wallet(&self) -> JsResult<Wallet> {
        Ok(Wallet {
            chains: wallet::Memory::default(),
            default: None,
            genesis_config: self.0.genesis_config().await?,
        })
    }

    // TODO(#40): figure out a way to alias or specify this string for TypeScript
    /// Claims a new chain from the faucet, with a new keypair and some tokens.
    ///
    /// # Errors
    /// - if we fail to get the list of current validators from the faucet
    /// - if we fail to claim the chain from the faucet
    /// - if we fail to persist the new chain or keypair to the wallet
    ///
    /// # Panics
    /// If an error occurs in the chain listener task.
    #[wasm_bindgen(js_name = claimChain)]
    pub async fn claim_chain(&self, wallet: &mut Wallet, owner: JsValue) -> JsResult<String> {
        let account_owner: AccountOwner = serde_wasm_bindgen::from_value(owner)?;
        tracing::info!(
            "Requesting a new chain for owner {} using the faucet at address {}",
            account_owner,
            self.0.url(),
        );
        let description = self.0.claim(&account_owner).await?;
        let chain_id = description.id();
        wallet.chains.insert(
            chain_id,
            wallet::Chain {
                owner: Some(account_owner),
                ..description.into()
            },
        );
        if wallet.default.is_none() {
            wallet.default = Some(chain_id);
        }
        Ok(chain_id.to_string())
    }
}

/// The full client API, exposed to the wallet implementation. Calls
/// to this API can be trusted to have originated from the user's
/// request.
#[wasm_bindgen]
#[derive(Clone)]
pub struct Client {
    // This use of `futures::lock::Mutex` is safe because we only
    // expose concurrency to the browser, which must always run all
    // futures on the global task queue.
    client_context: Arc<AsyncMutex<ClientContext>>,
}

#[derive(serde::Deserialize)]
struct TransferParams {
    donor: Option<AccountOwner>,
    amount: u64,
    recipient: linera_base::identifiers::Account,
}

#[wasm_bindgen]
impl Client {
    /// Creates a new client and connects to the network.
    ///
    /// # Errors
    /// On transport or protocol error, or if persistent storage is
    /// unavailable.
    #[wasm_bindgen(constructor)]
    pub async fn new(
        wallet: Wallet,
        signer: JsSigner,
        skip_process_inbox: bool,
        allow_application_logs: bool,
    ) -> Result<Client, JsError> {
        let mut storage = get_storage()
            .await?
            .with_allow_application_logs(allow_application_logs);
        wallet
            .genesis_config
            .initialize_storage(&mut storage)
            .await?;
        let client_context = ClientContext::new(
            storage.clone(),
            wallet.chains,
            signer,
            OPTIONS,
            wallet.default,
            wallet.genesis_config,
            BLOCK_CACHE_SIZE,
            EXECUTION_STATE_CACHE_SIZE,
        )
        .await?;
        // The `Arc` here is useless, but it is required by the `ChainListener` API.
        #[expect(clippy::arc_with_non_send_sync)]
        let client_context = Arc::new(AsyncMutex::new(client_context));
        let client_context_clone = client_context.clone();
        let chain_listener = ChainListener::new(
            ChainListenerConfig {
                skip_process_inbox,
                ..ChainListenerConfig::default()
            },
            client_context_clone,
            storage,
            tokio_util::sync::CancellationToken::new(),
            tokio::sync::mpsc::unbounded_channel().1,
        )
        .run(true) // Enable background sync
        .boxed_local()
        .await?
        .boxed_local();
        wasm_bindgen_futures::spawn_local(
            async move {
                if let Err(error) = chain_listener.await {
                    tracing::error!("ChainListener error: {error:?}");
                }
            }
            .boxed_local(),
        );
        log::info!("Linera Web client successfully initialized");
        Ok(Self { client_context })
    }

    /// Sets a callback to be called when a notification is received
    /// from the network.
    ///
    /// # Panics
    /// If the handler function fails or we fail to subscribe to the
    /// notification stream.
    #[wasm_bindgen(js_name = onNotification)]
    pub fn on_notification(&self, handler: js_sys::Function) {
        let this = self.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let mut notifications = this
                .default_chain_client()
                .await
                .unwrap()
                .subscribe()
                .unwrap();
            while let Some(notification) = notifications.next().await {
                tracing::debug!("received notification: {notification:?}");
                handler
                    .call1(
                        &JsValue::null(),
                        &serde_wasm_bindgen::to_value(&notification).unwrap(),
                    )
                    .unwrap();
            }
        });
    }

    async fn default_chain_client(&self) -> Result<ChainClient, JsError> {
        let client_context = self.client_context.lock().await;
        let chain_id = client_context.default_chain();
        Ok(client_context.make_chain_client(chain_id).await?)
    }

    async fn apply_client_command<Fut, T, E>(
        &self,
        chain_client: &ChainClient,
        mut command: impl FnMut() -> Fut,
    ) -> Result<Result<T, E>, linera_client::Error>
    where
        Fut: Future<Output = Result<ClientOutcome<T>, E>>,
    {
        let result = loop {
            use ClientOutcome::{Committed, WaitForTimeout};
            let timeout = match command().await {
                Ok(Committed(outcome)) => break Ok(Ok(outcome)),
                Ok(WaitForTimeout(timeout)) => timeout,
                Err(e) => break Ok(Err(e)),
            };
            let mut stream = chain_client.subscribe()?;
            linera_client::util::wait_for_next_round(&mut stream, timeout).await;
        };

        self.client_context
            .lock()
            .await
            .update_wallet(chain_client)
            .await?;

        result
    }

    /// Transfers funds from one account to another.
    ///
    /// `options` should be an options object of the form `{ donor,
    /// recipient, amount }`; omitting `donor` will cause the funds to
    /// come from the chain balance.
    ///
    /// # Errors
    /// - if the options object is of the wrong form
    /// - if the transfer fails
    #[wasm_bindgen]
    pub async fn transfer(&self, options: wasm_bindgen::JsValue) -> JsResult<()> {
        let params: TransferParams = serde_wasm_bindgen::from_value(options)?;
        let chain_client = self.default_chain_client().await?;

        let _hash = self
            .apply_client_command(&chain_client, || {
                chain_client.transfer(
                    params.donor.unwrap_or(AccountOwner::CHAIN),
                    linera_base::data_types::Amount::from_tokens(params.amount.into()),
                    params.recipient,
                )
            })
            .await??;

        Ok(())
    }

    /// Gets the balance of the default chain.
    ///
    /// # Errors
    /// If the chain couldn't be established.
    pub async fn balance(&self) -> JsResult<String> {
        Ok(self
            .default_chain_client()
            .await?
            .query_balance()
            .await?
            .to_string())
    }

    /// Gets the identity of the default chain.
    ///
    /// # Errors
    /// If the chain couldn't be established.
    pub async fn identity(&self) -> JsResult<JsValue> {
        Ok(serde_wasm_bindgen::to_value(
            &self.default_chain_client().await?.identity().await?,
        )?)
    }

    /// Gets the version information of the validators of the current network.
    ///
    /// # Errors
    /// If a validator is unreachable.
    ///
    /// # Panics
    /// If no default chain is set for the current wallet.
    #[wasm_bindgen(js_name = validatorVersionInfo)]
    pub async fn validator_version_info(&self) -> JsResult<JsValue> {
        let mut client_context = self.client_context.lock().await;
        let chain_id = client_context.default_chain();
        let chain_client = client_context.make_chain_client(chain_id).await?;
        chain_client.synchronize_from_validators().await?;
        let result = chain_client.local_committee().await;
        client_context.update_wallet(&chain_client).await?;
        let committee = result?;
        let node_provider = client_context.make_node_provider();

        let mut validator_versions = HashMap::new();

        for (name, state) in committee.validators() {
            match node_provider
                .make_node(&state.network_address)?
                .get_version_info()
                .await
            {
                Ok(version_info) => {
                    if validator_versions
                        .insert(name, version_info.clone())
                        .is_some()
                    {
                        tracing::warn!("duplicate validator entry for validator {name:?}");
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "failed to get version information for validator {name:?}:\n{e:?}"
                    );
                }
            }
        }

        Ok(validator_versions.serialize(&RESPONSE_SERIALIZER)?)
    }

    /// Retrieves an application for querying.
    ///
    /// # Errors
    /// If the application ID is invalid.
    #[wasm_bindgen]
    pub async fn application(&self, id: &str) -> JsResult<Application> {
        Ok(Application {
            client: self.clone(),
            id: id.parse()?,
        })
    }
}

// A serializer suitable for serializing responses to JavaScript to be
// sent using `postMessage`.
static RESPONSE_SERIALIZER: serde_wasm_bindgen::Serializer = serde_wasm_bindgen::Serializer::new()
    .serialize_large_number_types_as_bigints(true)
    .serialize_maps_as_objects(true);

#[wasm_bindgen]
pub struct Application {
    client: Client,
    id: ApplicationId,
}

#[wasm_bindgen]
impl Application {
    /// Performs a query against an application's service.
    ///
    /// If `block_hash` is non-empty, it specifies the block at which to
    /// perform the query; otherwise, the latest block is used.
    ///
    /// # Errors
    /// If the application ID is invalid, the query is incorrect, or
    /// the response isn't valid UTF-8.
    ///
    /// # Panics
    /// On internal protocol errors.
    #[wasm_bindgen]
    // TODO(#14) allow passing bytes here rather than just strings
    // TODO(#15) a lot of this logic is shared with `linera_service::node_service`
    pub async fn query(&self, query: &str, block_hash: Option<String>) -> JsResult<String> {
        tracing::debug!("querying application: {query}");
        let chain_client = self.client.default_chain_client().await?;
        let block_hash = if let Some(hash) = block_hash {
            Some(hash.as_str().parse()?)
        } else {
            None
        };
        let linera_execution::QueryOutcome {
            response: linera_execution::QueryResponse::User(response),
            operations,
        } = chain_client
            .query_application(
                linera_execution::Query::User {
                    application_id: self.id,
                    bytes: query.as_bytes().to_vec(),
                },
                block_hash,
            )
            .await?
        else {
            panic!("system response to user query")
        };

        if !operations.is_empty() {
            let _hash = self
                .client
                .apply_client_command(&chain_client, || {
                    chain_client.execute_operations(operations.clone(), vec![])
                })
                .await??;
        }

        Ok(String::from_utf8(response)?)
    }
}

#[wasm_bindgen(start)]
pub fn main() {
    use tracing_subscriber::{
        prelude::__tracing_subscriber_SubscriberExt as _, util::SubscriberInitExt as _,
    };

    std::panic::set_hook(Box::new(console_error_panic_hook::hook));

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .without_time()
                .with_writer(tracing_web::MakeWebConsoleWriter::new()),
        )
        .with(
            tracing_web::performance_layer()
                .with_details_from_fields(tracing_subscriber::fmt::format::Pretty::default()),
        )
        .init();
}
