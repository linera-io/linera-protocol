// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
# `linera-web`

This module defines the JavaScript bindings to the client API.

It is compiled to Wasm, with a JavaScript wrapper to inject its imports, and published on
NPM as `@linera/client`.

The `signer` subdirectory contains a TypeScript interface specifying the types of objects
that can be passed as signers — cryptographic integrations used to sign transactions, as
well as a demo implementation (not recommended for production use) that stores a private
key directly in memory and uses it to sign.
*/

// We sometimes need functions in this module to be async in order to
// ensure the generated code will return a `Promise`.
#![allow(clippy::unused_async)]
#![recursion_limit = "256"]

pub mod signer;
pub use signer::Signer;
pub mod wallet;
pub use wallet::Wallet;
pub mod faucet;
use std::{collections::HashMap, future::Future, rc::Rc, sync::Arc, time::Duration};

pub use faucet::Faucet;
use futures::{future::FutureExt as _, lock::Mutex as AsyncMutex, stream::StreamExt};
use linera_base::identifiers::{AccountOwner, ApplicationId};
use linera_client::{
    chain_listener::{ChainListener, ChainListenerConfig, ClientContext as _},
    client_options::ClientContextOptions,
};
use linera_core::{
    data_types::ClientOutcome,
    node::{ValidatorNode as _, ValidatorNodeProvider as _},
};
use linera_views::ViewError;
use serde::ser::Serialize as _;
use wasm_bindgen::prelude::*;
use web_sys::{js_sys, wasm_bindgen};

// TODO(#12): convert to IndexedDbStore once we refactor Context
type WebStorage =
    linera_storage::DbStorage<linera_views::memory::MemoryDatabase, linera_storage::WallClock>;

type WebEnvironment = linera_core::environment::Impl<
    WebStorage,
    linera_rpc::node_provider::NodeProvider,
    Signer,
    Rc<linera_core::wallet::Memory>,
>;

type JsResult<T> = Result<T, JsError>;

type ClientContext = linera_client::client_context::ClientContext<WebEnvironment>;
type ChainClient = linera_core::client::ChainClient<WebEnvironment>;

async fn get_storage() -> Result<WebStorage, ViewError> {
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

#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryOptions {
    block_hash: Option<String>,
    owner: Option<AccountOwner>,
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
        wallet: &Wallet,
        signer: Signer,
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
            wallet.chains.clone(),
            signer,
            OPTIONS,
            wallet.default,
            wallet.genesis_config.clone(),
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

    /// Adds a new owner to the default chain.
    ///
    /// # Errors
    ///
    /// If the owner is in the wrong format, or the chain client can't be instantiated.
    #[wasm_bindgen(js_name = addOwner)]
    pub async fn add_owner(&self, owner: JsValue, options: JsValue) -> JsResult<()> {
        #[derive(Default, serde::Deserialize)]
        struct Options {
            #[serde(default)]
            weight: u64,
        }

        let owner = serde_wasm_bindgen::from_value(owner)?;
        let Options { weight } =
            serde_wasm_bindgen::from_value::<Option<_>>(options)?.unwrap_or_default();
        let chain_client = self.default_chain_client().await?;
        self.apply_client_command(&chain_client, || {
            chain_client.share_ownership(owner, weight)
        })
        .await??;
        Ok(())
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
    pub async fn query(&self, query: &str, options: JsValue) -> JsResult<String> {
        tracing::debug!("querying application: {query}");
        let QueryOptions { block_hash, owner } =
            serde_wasm_bindgen::from_value::<Option<_>>(options)?.unwrap_or_default();
        let mut chain_client = self.client.default_chain_client().await?;
        if let Some(owner) = owner {
            chain_client.set_preferred_owner(owner);
        }
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
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    linera_base::tracing::init();
}
