/*!
This module defines the client API for the Web extension.
 */

// We sometimes need functions in this module to be async in order to
// ensure the generated code will return a `Promise`.
#![allow(clippy::unused_async)]

use std::{collections::HashMap, sync::Arc};

use futures::{lock::Mutex as AsyncMutex, stream::StreamExt};
use linera_client::{
    chain_listener::{ChainListener, ChainListenerConfig, ClientContext as _},
    client_options::ClientOptions,
    wallet::Wallet,
};
use linera_core::node::{ValidatorNode as _, ValidatorNodeProvider as _};
use linera_views::store::WithError;
use serde::ser::Serialize as _;
use wasm_bindgen::prelude::*;
use web_sys::{js_sys, wasm_bindgen};

// TODO(#12): convert to IndexedDbStore once we refactor Context
type WebStorage =
    linera_storage::DbStorage<linera_views::memory::MemoryStore, linera_storage::WallClock>;

async fn get_storage() -> Result<WebStorage, <linera_views::memory::MemoryStore as WithError>::Error>
{
    linera_storage::DbStorage::initialize(
        linera_views::memory::MemoryStoreConfig::new(1),
        "linera",
        b"",
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

#[wasm_bindgen(js_class = "Wallet")]
impl JsWallet {
    /// Creates and persists a new wallet from the given JSON string.
    ///
    /// # Errors
    /// If the wallet deserialization fails.
    #[wasm_bindgen]
    pub async fn create(wallet: &str) -> Result<JsWallet, JsError> {
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
    #[wasm_bindgen]
    pub fn on_notification(&self, handler: js_sys::Function) {
        let this = self.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let mut notifications = this
                .default_chain_client()
                .await
                .unwrap_throw()
                .subscribe()
                .await
                .unwrap_throw();
            while let Some(notification) = notifications.next().await {
                tracing::debug!("received notification: {notification:?}");
                handler
                    .call1(
                        &JsValue::null(),
                        &serde_wasm_bindgen::to_value(&notification).unwrap_throw(),
                    )
                    .unwrap_throw();
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
impl Frontend {
    /// Gets the version information of the validators of the current network.
    ///
    /// # Errors
    /// If a validator is unreachable.
    ///
    /// # Panics
    /// If no default chain is set for the current wallet.
    #[wasm_bindgen]
    pub async fn validator_version_info(&self) -> Result<JsValue, JsError> {
        let mut client_context = self.0.client_context.lock().await;
        let chain_id = client_context
            .wallet()
            .default_chain()
            .expect("No default chain");
        let chain_client = client_context.make_chain_client(chain_id)?;
        chain_client.synchronize_from_validators().await?;
        let result = chain_client.local_committee().await;
        client_context.update_and_save_wallet(&chain_client).await?;
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

    /// Performs a query against an application's service.
    ///
    /// # Errors
    /// If the application ID is invalid, the query is incorrect, or
    /// the response isn't valid UTF-8.
    ///
    /// # Panics
    /// On internal protocol errors.
    #[wasm_bindgen]
    // TODO(14) allow passing bytes here rather than just strings
    // TODO(15) a lot of this logic is shared with `linera_service::node_service`
    pub async fn query_application(
        &self,
        application_id: &str,
        query: &str,
    ) -> Result<String, JsError> {
        let chain_client = self.0.default_chain_client().await?;
        let response = chain_client
            .query_application(linera_execution::Query::User {
                application_id: application_id.parse()?,
                bytes: query.as_bytes().to_vec(),
            })
            .await?;
        let linera_execution::Response::User(response) = response else {
            panic!("system response to user query")
        };
        Ok(String::from_utf8(response)?)
    }

    /// Mutate an application's state with the given mutation.
    ///
    /// # Errors
    /// If the application ID or mutation is invalid.
    ///
    /// # Panics
    /// If the response from the service is not a GraphQL response
    /// containing operations to execute.
    #[wasm_bindgen]
    // TODO(linera-protocol#2911) this function assumes GraphQL service output
    pub async fn mutate_application(
        &self,
        application_id: &str,
        mutation: &str,
    ) -> Result<(), JsError> {
        fn array_to_bytes(array: &[serde_json::Value]) -> Vec<u8> {
            array
                .iter()
                .map(|value| value.as_u64().unwrap().try_into().unwrap())
                .collect()
        }

        let chain_client = self.0.default_chain_client().await?;
        let application_id = application_id.parse()?;
        let response = chain_client
            .query_application(linera_execution::Query::User {
                application_id,
                bytes: mutation.as_bytes().to_vec(),
            })
            .await?;
        let linera_execution::Response::User(response) = response else {
            panic!("system response to user query")
        };
        let response: serde_json::Value = serde_json::from_slice(&response)?;
        let data = &response["data"];
        tracing::info!("data: {data:?}");
        let operations: Vec<_> = data
            .as_object()
            .unwrap()
            .values()
            .map(|value| linera_execution::Operation::User {
                application_id,
                bytes: array_to_bytes(value.as_array().unwrap()),
            })
            .collect();

        let _hash = loop {
            use linera_core::data_types::ClientOutcome::{Committed, WaitForTimeout};
            let timeout = match chain_client.execute_operations(operations.clone()).await? {
                Committed(certificate) => break certificate.value().hash(),
                WaitForTimeout(timeout) => timeout,
            };
            let mut stream = chain_client.subscribe().await?;
            linera_client::util::wait_for_next_round(&mut stream, timeout).await;
        };

        Ok(())
    }
}

#[wasm_bindgen(start)]
pub fn main() {
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    linera_base::tracing::init();
}
