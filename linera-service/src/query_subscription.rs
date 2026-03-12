// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures::StreamExt as _;
use linera_base::identifiers::{ApplicationId, ChainId};
use linera_client::chain_listener::ClientContext;
use linera_core::worker::Reason;
use linera_execution::{Query, QueryResponse};
use serde_json::Value;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// A named GraphQL query string registered at startup via `--allow-subscription`.
#[derive(Clone, Debug)]
pub struct RegisteredQuery {
    pub name: String,
    pub query: String,
}

/// Parses a GraphQL query string like `query Name { ... }` and extracts the operation name.
pub fn parse_allowed_subscription(s: &str) -> anyhow::Result<RegisteredQuery> {
    let trimmed = s.trim();
    let rest = trimmed
        .strip_prefix("query")
        .ok_or_else(|| anyhow::anyhow!("expected query to start with 'query', got: {s}"))?;
    // The character right after "query" must be whitespace (not part of a longer word).
    anyhow::ensure!(
        rest.starts_with(char::is_whitespace),
        "expected whitespace after 'query' keyword"
    );
    let rest = rest.trim_start();
    // Extract the operation name: sequence of alphanumeric/underscore chars.
    let name = rest
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .next()
        .unwrap_or_default();
    anyhow::ensure!(
        !name.is_empty(),
        "expected an operation name after 'query', e.g. 'query MyQuery {{ ... }}'"
    );
    Ok(RegisteredQuery {
        name: name.to_string(),
        query: trimmed.to_string(),
    })
}

/// Identifies a unique subscription target: a named query for a specific chain and application.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct SubscriptionKey {
    pub name: String,
    pub chain_id: ChainId,
    pub application_id: ApplicationId,
}

/// State for an active watcher: the broadcast sender and a handle for cleanup detection.
struct WatcherState {
    sender: broadcast::Sender<Value>,
}

/// Manages registered query names and active per-key watchers.
pub struct QuerySubscriptionManager {
    /// Registered queries by name.
    queries: HashMap<String, String>,
    /// Active watchers keyed by subscription target.
    watchers: Mutex<HashMap<SubscriptionKey, WatcherState>>,
}

impl QuerySubscriptionManager {
    /// Creates a new manager from the registered queries.
    pub fn new(registered: Vec<RegisteredQuery>) -> Self {
        let queries = registered
            .into_iter()
            .map(|rq| (rq.name, rq.query))
            .collect();
        Self {
            queries,
            watchers: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the GraphQL query string for a given name, if registered.
    pub fn get_query(&self, name: &str) -> Option<&str> {
        self.queries.get(name).map(|s| s.as_str())
    }

    /// Returns a broadcast receiver for the given key. Lazily spawns a watcher if needed.
    pub fn subscribe<C: ClientContext + 'static>(
        self: &Arc<Self>,
        key: &SubscriptionKey,
        context: Arc<futures::lock::Mutex<C>>,
        token: CancellationToken,
    ) -> anyhow::Result<broadcast::Receiver<Value>> {
        let query_string = self
            .get_query(&key.name)
            .ok_or_else(|| {
                anyhow::anyhow!("no subscription query registered with name '{}'", key.name)
            })?
            .to_string();

        let mut watchers = self.watchers.lock().unwrap();

        // If a watcher already exists, reuse it.
        if let Some(state) = watchers.get(key) {
            return Ok(state.sender.subscribe());
        }

        // Create a new broadcast channel and spawn a watcher.
        let (sender, receiver) = broadcast::channel(64);
        watchers.insert(
            key.clone(),
            WatcherState {
                sender: sender.clone(),
            },
        );

        let manager = Arc::clone(self);
        let key_clone = key.clone();
        tokio::spawn(run_query_subscription_watcher(
            context,
            manager,
            key_clone,
            query_string,
            sender,
            token,
        ));

        Ok(receiver)
    }
}

/// Background task that watches for new blocks on a chain and re-executes the query.
async fn run_query_subscription_watcher<C: ClientContext + 'static>(
    context: Arc<futures::lock::Mutex<C>>,
    manager: Arc<QuerySubscriptionManager>,
    key: SubscriptionKey,
    query_string: String,
    sender: broadcast::Sender<Value>,
    token: CancellationToken,
) {
    debug!(
        name = %key.name,
        chain_id = %key.chain_id,
        application_id = %key.application_id,
        "starting query subscription watcher"
    );

    let notification_stream = {
        let ctx = context.lock().await;
        match ctx.make_chain_client(key.chain_id).await {
            Ok(client) => match client.subscribe() {
                Ok(stream) => stream,
                Err(e) => {
                    warn!("failed to subscribe to chain notifications: {e}");
                    cleanup_watcher(&manager, &key);
                    return;
                }
            },
            Err(e) => {
                warn!("failed to create chain client: {e}");
                cleanup_watcher(&manager, &key);
                return;
            }
        }
    };

    let mut notification_stream = Box::pin(notification_stream);

    // Cache the last result to deduplicate.
    let mut last_result: Option<Vec<u8>> = None;

    // Execute the query once immediately so the first subscriber gets a value.
    execute_and_maybe_broadcast(&context, &key, &query_string, &sender, &mut last_result).await;

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                debug!(name = %key.name, "watcher cancelled");
                break;
            }
            notification = notification_stream.next() => {
                match notification {
                    Some(n) => {
                        if matches!(n.reason, Reason::NewBlock { .. }) {
                            execute_and_maybe_broadcast(
                                &context,
                                &key,
                                &query_string,
                                &sender,
                                &mut last_result,
                            )
                            .await;
                        }
                    }
                    None => {
                        debug!(name = %key.name, "notification stream ended");
                        break;
                    }
                }

                // If no receivers remain, stop the watcher.
                if sender.receiver_count() == 0 {
                    debug!(name = %key.name, "no more subscribers, stopping watcher");
                    break;
                }
            }
        }
    }

    cleanup_watcher(&manager, &key);
}

/// Executes the query against the application and broadcasts if the result changed.
async fn execute_and_maybe_broadcast<C: ClientContext + 'static>(
    context: &Arc<futures::lock::Mutex<C>>,
    key: &SubscriptionKey,
    query_string: &str,
    sender: &broadcast::Sender<Value>,
    last_result: &mut Option<Vec<u8>>,
) {
    // The application service expects a JSON-encoded GraphQL request.
    let json_request = serde_json::json!({ "query": query_string });
    let request_bytes = serde_json::to_vec(&json_request).unwrap();
    let query = Query::User {
        application_id: key.application_id,
        bytes: request_bytes,
    };

    let result = {
        let ctx = context.lock().await;
        match ctx.make_chain_client(key.chain_id).await {
            Ok(client) => client.query_application(query, None).await,
            Err(e) => {
                warn!(name = %key.name, "failed to create chain client: {e}");
                return;
            }
        }
    };

    match result {
        Ok((outcome, _height)) => {
            let response_bytes = match outcome.response {
                QueryResponse::User(bytes) => bytes,
                QueryResponse::System(_) => {
                    warn!(name = %key.name, "unexpected system response for user query");
                    return;
                }
            };

            // Deduplicate: only broadcast if the result changed.
            if last_result.as_ref() == Some(&response_bytes) {
                return;
            }

            *last_result = Some(response_bytes.clone());

            // Parse the response as JSON and broadcast.
            match serde_json::from_slice::<Value>(&response_bytes) {
                Ok(value) => {
                    // Ignore send errors (no receivers).
                    if let Err(e) = sender.send(value) {
                        debug!(name = %key.name, "Failed to send graphql response: {e}");
                    }
                }
                Err(e) => {
                    warn!(name = %key.name, "failed to parse query response as JSON: {e}");
                }
            }
        }
        Err(e) => {
            warn!(name = %key.name, "query execution failed: {e}");
        }
    }
}

/// Removes the watcher entry from the manager.
fn cleanup_watcher(manager: &QuerySubscriptionManager, key: &SubscriptionKey) {
    let mut watchers = manager.watchers.lock().unwrap();
    watchers.remove(key);
    debug!(name = %key.name, "watcher cleaned up");
}
