/*!
This module defines the client API for the Web extension.
 */

// We sometimes need functions in this module to be async in order to
// ensure the generated code will return a `Promise`.
#![allow(clippy::unused_async)]

use std::{collections::HashMap, future::Future, sync::Arc};

use futures::{lock::Mutex as AsyncMutex, stream::StreamExt};
use linera_base::identifiers::ApplicationId;
use linera_client::{
    chain_listener::{ChainListener, ChainListenerConfig, ClientContext as _},
    client_options::ClientOptions,
    wallet::Wallet,
};
use linera_core::{
    data_types::ClientOutcome,
    node::{ValidatorNode as _, ValidatorNodeProvider as _},
};
use linera_faucet_client::Faucet;
use linera_views::store::WithError;
use serde::ser::Serialize as _;
use wasm_bindgen::prelude::*;
use web_sys::{js_sys, wasm_bindgen};

// TODO(#12): convert to IndexedDbStore once we refactor Context
type WebStorage =
    linera_storage::DbStorage<linera_views::memory::MemoryStore, linera_storage::WallClock>;

type JsResult<T> = Result<T, JsError>;

async fn get_storage() -> Result<WebStorage, <linera_views::memory::MemoryStore as WithError>::Error>
{
    linera_storage::DbStorage::initialize(
        linera_views::memory::MemoryStoreConfig::new(1),
        "linera",
        Some(linera_execution::WasmRuntime::Wasmer),
    )
    .await
}

type PersistentWallet = linera_client::persistent::Memory<Wallet>;
type ClientContext = linera_client::client_context::ClientContext<WebStorage, PersistentWallet>;
type ChainClient =
    linera_core::client::ChainClient<linera_rpc::node_provider::NodeProvider, WebStorage>;

// TODO(#13): get from user
pub const OPTIONS: ClientOptions = ClientOptions {
    send_timeout: std::time::Duration::from_millis(4000),
    recv_timeout: std::time::Duration::from_millis(4000),
    max_pending_message_bundles: 10,
    wasm_runtime: Some(linera_execution::WasmRuntime::Wasmer),
    max_concurrent_queries: None,
    max_loaded_chains: nonzero_lit::usize!(40),
    max_stream_queries: 10,
    cache_size: 1000,
    retry_delay: std::time::Duration::from_millis(1000),
    max_retries: 10,
    wait_for_outgoing_messages: false,
    blanket_message_policy: linera_core::client::BlanketMessagePolicy::Accept,
    restrict_chain_ids_to: None,
    long_lived_services: false,
    blob_download_timeout: std::time::Duration::from_millis(1000),
    grace_period: linera_core::DEFAULT_GRACE_PERIOD,

    // TODO(linera-protocol#2944): separate these out from the
    // `ClientOptions` struct, since they apply only to the CLI/native
    // client
    tokio_threads: Some(1),
    command: linera_client::client_options::ClientCommand::Keygen,
    wallet_state_path: None,
    storage_config: None,
    with_wallet: None,
};

#[wasm_bindgen(js_name = Wallet)]
pub struct JsWallet(PersistentWallet);

#[wasm_bindgen(js_name = Faucet)]
pub struct JsFaucet(Faucet);

#[wasm_bindgen(js_class = "Faucet")]
impl JsFaucet {
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new(url: String) -> JsFaucet {
        JsFaucet(Faucet::new(url))
    }

    /// Creates a new wallet from the faucet.
    ///
    /// # Errors
    /// If we couldn't retrieve the genesis config from the faucet.
    #[wasm_bindgen(js_name = createWallet)]
    pub async fn create_wallet(&self) -> JsResult<JsWallet> {
        Ok(JsWallet(PersistentWallet::new(
            linera_client::wallet::Wallet::new(self.0.genesis_config().await?, None),
        )))
    }

    // TODO(#40): figure out a way to alias or specify this string for TypeScript
    /// Claims a new chain from the faucet, with a new keypair and some tokens.
    ///
    /// # Errors
    /// - if we fail to get the list of current validators from the faucet
    /// - if we fail to claim the chain from the faucet
    /// - if we fail to persist the new chain or keypair to the wallet
    #[wasm_bindgen(js_name = claimChain)]
    pub async fn claim_chain(&self, client: &mut Client) -> JsResult<String> {
        use linera_client::persistent::LocalPersistExt as _;
        let mut context = client.client_context.lock().await;
        let key_pair = context.wallet.generate_key_pair();
        let owner: linera_base::identifiers::Owner = key_pair.public().into();
        tracing::info!(
            "Requesting a new chain for owner {} using the faucet at address {}",
            owner,
            self.0.url(),
        );
        context
            .wallet
            .mutate(|wallet| wallet.add_unassigned_key_pair(key_pair))
            .await?;
        let outcome = self.0.claim(&owner).await?;
        let validators = self.0.current_validators().await?;
        context
            .assign_new_chain_to_key(
                outcome.chain_id,
                outcome.message_id,
                owner,
                Some(validators),
            )
            .await?;
        context
            .wallet
            .mutate(|wallet| wallet.set_default_chain(outcome.chain_id))
            .await??;
        context.client.track_chain(outcome.chain_id);
        let chain_client = context.make_chain_client(outcome.chain_id)?;
        wasm_bindgen_futures::spawn_local(async move {
            let (listener, listen_handle, mut notifications) = chain_client.listen().await.unwrap();
            wasm_bindgen_futures::spawn_local(async move {
                while let Some(notification) = notifications.next().await {
                    if let linera_core::worker::Reason::NewIncomingBundle { .. } =
                        notification.reason
                    {
                        chain_client.process_inbox().await.unwrap();
                    }
                }
            });
            listener.await;
            drop(listen_handle);
        });
        Ok(outcome.chain_id.to_string())
    }
}

#[wasm_bindgen(js_class = "Wallet")]
impl JsWallet {
    /// Creates and persists a new wallet from the given JSON string.
    ///
    /// # Errors
    /// If the wallet deserialization fails.
    #[wasm_bindgen(js_name = fromJson)]
    pub async fn from_json(wallet: &str) -> Result<JsWallet, JsError> {
        Ok(JsWallet(PersistentWallet::new(serde_json::from_str(
            wallet,
        )?)))
    }

    /// Attempts to read the wallet from persistent storage.
    ///
    /// # Errors
    /// If storage is inaccessible.
    #[wasm_bindgen]
    pub async fn read() -> Result<Option<JsWallet>, JsError> {
        Ok(None)
    }
}

/// The full client API, exposed to the wallet implementation. Calls
/// to this API can be trusted to have originated from the user's
/// request. This struct is the backend for the extension itself
/// (side panel, option page, et cetera).
#[wasm_bindgen]
#[derive(Clone)]
pub struct Client {
    // This use of `futures::lock::Mutex` is safe because we only
    // expose concurrency to the browser, which must always run all
    // futures on the global task queue.
    client_context: Arc<AsyncMutex<ClientContext>>,
}

/// The subset of the client API that should be exposed to application
/// frontends. Any function exported here with `wasm_bindgen` can be
/// called by untrusted Web pages, and so inputs must be verified and
/// outputs must not leak sensitive information without user
/// confirmation.
#[wasm_bindgen]
#[derive(Clone)]
pub struct Frontend(Client);

#[derive(serde::Deserialize)]
struct TransferParams {
    donor: Option<linera_base::identifiers::Owner>,
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
    pub async fn new(wallet: JsWallet) -> Result<Client, JsError> {
        let JsWallet(wallet) = wallet;
        let mut storage = get_storage().await?;
        wallet
            .genesis_config()
            .initialize_storage(&mut storage)
            .await?;
        let client_context = Arc::new(AsyncMutex::new(ClientContext::new(
            storage.clone(),
            OPTIONS,
            wallet,
        )));
        ChainListener::new(ChainListenerConfig::default())
            .run(client_context.clone(), storage)
            .await;
        log::info!("Linera Web client successfully initialized");
        Ok(Self { client_context })
    }

    /// Set a callback to be called when a notification is received
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
                .await
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
        let chain_id = client_context
            .wallet()
            .default_chain()
            .expect("A default chain should be configured");
        Ok(client_context.make_chain_client(chain_id)?)
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
            let mut stream = chain_client.subscribe().await?;
            linera_client::util::wait_for_next_round(&mut stream, timeout).await;
        };

        self.client_context
            .lock()
            .await
            .update_wallet(chain_client)
            .await?;

        result
    }

    #[wasm_bindgen]
    pub async fn transfer(&self, options: wasm_bindgen::JsValue) -> JsResult<()> {
        let params: TransferParams = serde_wasm_bindgen::from_value(options)?;
        let chain_client = self.default_chain_client().await?;

        let _hash = self
            .apply_client_command(&chain_client, || {
                chain_client.transfer(
                    params.donor,
                    linera_base::data_types::Amount::from_tokens(params.amount.into()),
                    linera_execution::system::Recipient::Account(params.recipient),
                )
            })
            .await??;

        Ok(())
    }

    pub async fn identity(&self) -> JsResult<JsValue> {
        Ok(serde_wasm_bindgen::to_value(
            &self.default_chain_client().await?.identity().await?,
        )?)
    }

    /// Gets an object implementing the API for Web frontends.
    #[wasm_bindgen]
    #[must_use]
    pub fn frontend(&self) -> Frontend {
        Frontend(self.clone())
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
impl Frontend {
    /// Gets the version information of the validators of the current network.
    ///
    /// # Errors
    /// If a validator is unreachable.
    ///
    /// # Panics
    /// If no default chain is set for the current wallet.
    #[wasm_bindgen(js_name = validatorVersionInfo)]
    pub async fn validator_version_info(&self) -> JsResult<JsValue> {
        let mut client_context = self.0.client_context.lock().await;
        let chain_id = client_context
            .wallet()
            .default_chain()
            .expect("No default chain");
        let chain_client = client_context.make_chain_client(chain_id)?;
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
            client: self.0.clone(),
            id: id.parse()?,
        })
    }
}

#[wasm_bindgen]
impl Application {
    /// Performs a query against an application's service.
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
    pub async fn query(&self, query: &str) -> JsResult<String> {
        let chain_client = self.client.default_chain_client().await?;

        let linera_execution::QueryOutcome {
            response: linera_execution::QueryResponse::User(response),
            operations,
        } = chain_client
            .query_application(linera_execution::Query::User {
                application_id: self.id,
                bytes: query.as_bytes().to_vec(),
            })
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
