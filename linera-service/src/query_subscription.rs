// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::StreamExt as _;
use linera_base::identifiers::{ApplicationId, ChainId};
use linera_client::chain_listener::ClientContext;
use linera_core::worker::Reason;
use linera_execution::{Query, QueryResponse};
use tokio::sync::watch;
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

/// Parses a `Name=Secs` string into a query name and TTL in seconds.
pub fn parse_subscription_ttl(s: &str) -> Result<(String, u64), String> {
    let (name, secs) = s
        .split_once('=')
        .ok_or_else(|| format!("expected format Name=Secs, got: {s}"))?;
    let secs: u64 = secs
        .parse()
        .map_err(|e| format!("invalid seconds value '{secs}': {e}"))?;
    Ok((name.to_string(), secs))
}

/// Identifies a unique subscription target: a named query for a specific chain and application.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct SubscriptionKey {
    pub name: String,
    pub chain_id: ChainId,
    pub application_id: ApplicationId,
}

/// State for an active watcher: the watch sender for the latest query result.
/// The channel carries pre-serialized JSON strings so that cloning between
/// subscribers is a single `memcpy` instead of a deep `serde_json::Value` clone.
struct WatcherState {
    sender: watch::Sender<Option<String>>,
}

/// Manages registered query names and active per-key watchers.
pub struct QuerySubscriptionManager {
    /// Registered queries by name.
    queries: HashMap<String, String>,
    /// Per-query minimum TTL for cached results.
    ttls: HashMap<String, Duration>,
    /// Active watchers keyed by subscription target.
    watchers: Mutex<HashMap<SubscriptionKey, WatcherState>>,
}

impl QuerySubscriptionManager {
    /// Creates a new manager from the registered queries and optional per-query TTLs.
    pub fn new(registered: Vec<RegisteredQuery>, ttls: HashMap<String, Duration>) -> Self {
        let queries = registered
            .into_iter()
            .map(|rq| (rq.name, rq.query))
            .collect();
        Self {
            queries,
            ttls,
            watchers: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the GraphQL query string for a given name, if registered.
    pub fn get_query(&self, name: &str) -> Option<&str> {
        self.queries.get(name).map(|s| s.as_str())
    }

    /// Returns a watch receiver for the given key. Lazily spawns a watcher if needed.
    /// The receiver initially holds `None`; the watcher populates it with `Some(value)`
    /// after the first query. Callers should filter out `None` values from the stream.
    pub fn subscribe<C: ClientContext + 'static>(
        self: &Arc<Self>,
        key: &SubscriptionKey,
        context: Arc<futures::lock::Mutex<C>>,
        token: CancellationToken,
    ) -> anyhow::Result<watch::Receiver<Option<String>>> {
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

        // Create a new watch channel (initial value is None until the first query completes).
        let (sender, receiver) = watch::channel(None);
        watchers.insert(
            key.clone(),
            WatcherState {
                sender: sender.clone(),
            },
        );

        let ttl = self.ttls.get(&key.name).copied();
        let manager = Arc::clone(self);
        let key_clone = key.clone();
        tokio::spawn(run_query_subscription_watcher(
            context,
            manager,
            key_clone,
            query_string,
            sender,
            token,
            ttl,
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
    sender: watch::Sender<Option<String>>,
    token: CancellationToken,
    ttl: Option<Duration>,
) {
    debug!(
        name = %key.name,
        chain_id = %key.chain_id,
        application_id = %key.application_id,
        ?ttl,
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

    // Cache the last result as a string to deduplicate.
    let mut last_result: Option<String> = None;

    // Execute the query once immediately so the first subscriber gets a value.
    execute_and_maybe_send(&context, &key, &query_string, &sender, &mut last_result).await;

    // Track when the last execution happened, for TTL-based deferral.
    let mut last_execution = tokio::time::Instant::now();
    // Whether a deferred re-execution is pending (a NewBlock arrived during the TTL window).
    let mut pending_invalidation = false;

    loop {
        // If there's a pending invalidation, compute the remaining TTL sleep.
        let ttl_sleep = if pending_invalidation {
            if let Some(ttl) = ttl {
                let elapsed = last_execution.elapsed();
                if elapsed < ttl {
                    tokio::time::sleep(ttl - elapsed)
                } else {
                    tokio::time::sleep(Duration::ZERO)
                }
            } else {
                // No TTL configured; should not happen since pending_invalidation
                // is only set when ttl is Some, but handle gracefully.
                tokio::time::sleep(Duration::ZERO)
            }
        } else {
            // No pending invalidation: sleep forever (effectively disabled).
            tokio::time::sleep(Duration::MAX)
        };
        tokio::pin!(ttl_sleep);

        tokio::select! {
            _ = token.cancelled() => {
                debug!(name = %key.name, "watcher cancelled");
                break;
            }
            () = &mut ttl_sleep, if pending_invalidation => {
                pending_invalidation = false;
                execute_and_maybe_send(
                    &context,
                    &key,
                    &query_string,
                    &sender,
                    &mut last_result,
                )
                .await;
                last_execution = tokio::time::Instant::now();
            }
            notification = notification_stream.next() => {
                match notification {
                    Some(n) => {
                        if matches!(n.reason, Reason::NewBlock { .. }) {
                            if ttl.is_some() {
                                // Defer re-execution until the TTL expires.
                                if !pending_invalidation {
                                    debug!(name = %key.name, "deferring invalidation until TTL expires");
                                }
                                pending_invalidation = true;
                            } else {
                                execute_and_maybe_send(
                                    &context,
                                    &key,
                                    &query_string,
                                    &sender,
                                    &mut last_result,
                                )
                                .await;
                                last_execution = tokio::time::Instant::now();
                            }
                        }
                    }
                    None => {
                        debug!(name = %key.name, "notification stream ended");
                        break;
                    }
                }

                // If no receivers remain, stop the watcher.
                if sender.is_closed() {
                    debug!(name = %key.name, "no more subscribers, stopping watcher");
                    break;
                }
            }
        }
    }

    cleanup_watcher(&manager, &key);
}

/// Executes the query against the application and updates the watch channel if the result changed.
async fn execute_and_maybe_send<C: ClientContext + 'static>(
    context: &Arc<futures::lock::Mutex<C>>,
    key: &SubscriptionKey,
    query_string: &str,
    sender: &watch::Sender<Option<String>>,
    last_result: &mut Option<String>,
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

            // Convert to string. The bytes are already valid UTF-8 JSON
            // from the application service.
            let json_string = match String::from_utf8(response_bytes) {
                Ok(s) => s,
                Err(error) => {
                    warn!(%error, name = %key.name, "response bytes are not valid UTF-8");
                    return;
                }
            };

            // Deduplicate: only send if the result changed.
            if last_result.as_ref() == Some(&json_string) {
                return;
            }

            if let Err(e) = sender.send(Some(json_string.clone())) {
                debug!(name = %key.name, "Failed to send graphql response: {e}");
            }
            *last_result = Some(json_string);
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
