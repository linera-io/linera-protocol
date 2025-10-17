// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use linera_base::time::{Duration, Instant};
use tokio::sync::broadcast;

use super::request::{RequestKey, RequestResult};
use crate::node::NodeError;

/// Tracks in-flight requests to deduplicate concurrent requests for the same data.
///
/// This structure manages a map of request keys to in-flight entries, each containing
/// broadcast senders for notifying waiters when a request completes, as well as timing
/// information and alternative data sources.
#[derive(Debug, Clone)]
pub(super) struct InFlightTracker<N> {
    /// Maps request keys to in-flight entries containing broadcast senders and metadata
    entries: Arc<tokio::sync::RwLock<HashMap<RequestKey, InFlightEntry<N>>>>,
    /// Maximum duration before an in-flight request is considered stale and deduplication is skipped
    timeout: Duration,
}

impl<N: Clone> InFlightTracker<N> {
    /// Creates a new `InFlightTracker` with the specified timeout.
    ///
    /// # Arguments
    /// - `timeout`: Maximum duration before an in-flight request is considered too old to deduplicate against
    pub(super) fn new(timeout: Duration) -> Self {
        Self {
            entries: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            timeout,
        }
    }

    /// Attempts to subscribe to an existing in-flight request (exact or subsuming match).
    ///
    /// Searches for either an exact key match or a subsuming request (whose result would
    /// contain all the data needed by this request). Returns information about which type
    /// of match was found, along with subscription details.
    ///
    /// # Arguments
    /// - `key`: The request key to look up
    ///
    /// # Returns
    /// - `None`: No matching in-flight request found
    /// - `Some(InFlightMatch::Exact(outcome))`: Exact key match found
    /// - `Some(InFlightMatch::Subsuming { key, outcome })`: Subsuming request found
    pub(super) async fn try_subscribe(&self, key: &RequestKey) -> Option<InFlightMatch> {
        let in_flight = self.entries.write().await;

        // First check for exact match
        if let Some(entry) = in_flight.get(key) {
            let elapsed = Instant::now().duration_since(entry.started_at);

            let outcome = if elapsed > self.timeout {
                SubscribeOutcome::TimedOut(elapsed)
            } else {
                SubscribeOutcome::Subscribed(entry.sender.subscribe())
            };

            return Some(InFlightMatch::Exact(outcome));
        }

        // Check for subsuming request
        for (in_flight_key, entry) in in_flight.iter() {
            if in_flight_key.subsumes(key) {
                let elapsed = Instant::now().duration_since(entry.started_at);

                let outcome = if elapsed > self.timeout {
                    SubscribeOutcome::TimedOut(elapsed)
                } else {
                    SubscribeOutcome::Subscribed(entry.sender.subscribe())
                };

                return Some(InFlightMatch::Subsuming {
                    key: in_flight_key.clone(),
                    outcome,
                });
            }
        }

        None
    }

    /// Inserts a new in-flight request entry.
    ///
    /// Creates a new broadcast channel and in-flight entry for the given key,
    /// marking the start time as now.
    ///
    /// # Arguments
    /// - `key`: The request key to insert
    pub(super) async fn insert_new(&self, key: RequestKey) {
        let (sender, _receiver) = broadcast::channel(1);
        let mut in_flight = self.entries.write().await;

        in_flight.insert(
            key,
            InFlightEntry {
                sender,
                started_at: Instant::now(),
                alternative_peers: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            },
        );
    }

    /// Completes an in-flight request by removing it and broadcasting the result.
    ///
    /// Removes the entry for the given key and broadcasts the result to all waiting
    /// subscribers. Logs the number of waiters that received the notification.
    ///
    /// # Arguments
    /// - `key`: The request key to complete
    /// - `result`: The result to broadcast to waiters
    pub(super) async fn complete_and_broadcast(
        &self,
        key: &RequestKey,
        result: Arc<Result<RequestResult, NodeError>>,
    ) -> usize {
        let mut in_flight = self.entries.write().await;

        if let Some(entry) = in_flight.remove(key) {
            let waiter_count = entry.sender.receiver_count();
            tracing::info!(
                key = ?key,
                waiters = waiter_count,
                "request completed; broadcasting result to waiters",
            );
            if waiter_count != 0 {
                let _ = entry.sender.send(result);
            }
            return waiter_count;
        }
        0
    }

    /// Registers an alternative peer for an in-flight request and checks if it has timed out.
    ///
    /// If an entry exists for the given key, registers the peer as an alternative source
    /// (if not already registered) and returns the elapsed time since the request started.
    /// The caller provides a predicate to check if the peer is already registered.
    ///
    /// # Arguments
    /// - `key`: The request key
    /// - `peer`: The peer to register as an alternative
    /// - `already_registered`: Predicate to check if peer is already in the list
    ///
    /// # Returns
    /// - `Some(elapsed)`: Entry exists; returns elapsed time since request started
    /// - `None`: No entry exists for this key
    pub(super) async fn register_alternative_and_check_timeout(
        &self,
        key: &RequestKey,
        peer: N,
    ) -> Option<Duration>
    where
        N: PartialEq + Eq,
    {
        let in_flight = self.entries.read().await;

        if let Some(entry) = in_flight.get(key) {
            let elapsed = Instant::now().duration_since(entry.started_at);

            // Register this peer as an alternative source if not already present
            {
                let mut alt_peers = entry.alternative_peers.write().await;
                if !alt_peers.iter().any(|p| *p == peer) {
                    alt_peers.push(peer);
                }
            }

            return Some(elapsed);
        }

        None
    }

    /// Retrieves the list of alternative peers registered for an in-flight request.
    ///
    /// Returns a clone of the alternative peers list if an entry exists for the given key.
    ///
    /// # Arguments
    /// - `key`: The request key to look up
    ///
    /// # Returns
    /// - `Some(peers)`: Entry exists; returns cloned list of alternative peers
    /// - `None`: No entry exists for this key
    pub(super) async fn get_alternative_peers(&self, key: &RequestKey) -> Option<Vec<N>> {
        let in_flight = self.entries.read().await;

        if let Some(entry) = in_flight.get(key) {
            let peers = entry.alternative_peers.read().await;
            Some(peers.clone())
        } else {
            None
        }
    }
}

/// Type of in-flight request match found.
#[derive(Debug)]
pub(super) enum InFlightMatch {
    /// Exact key match found
    Exact(SubscribeOutcome),
    /// Subsuming key match found (larger request that contains this request)
    Subsuming {
        /// The key of the subsuming request
        key: RequestKey,
        /// Outcome of attempting to subscribe
        outcome: SubscribeOutcome,
    },
}

/// Outcome of attempting to subscribe to an in-flight request.
#[derive(Debug)]
pub(super) enum SubscribeOutcome {
    /// Successfully subscribed; receiver will be notified when request completes
    Subscribed(broadcast::Receiver<Arc<Result<RequestResult, NodeError>>>),
    /// Request exists but has exceeded the timeout threshold
    TimedOut(Duration),
}

/// In-flight request entry that tracks when the request was initiated.
#[derive(Debug)]
pub(super) struct InFlightEntry<N> {
    /// Broadcast sender for notifying waiters when the request completes
    sender: broadcast::Sender<Arc<Result<RequestResult, NodeError>>>,
    /// Time when this request was initiated
    started_at: Instant,
    /// Alternative peers that can provide this data if the primary request fails
    alternative_peers: Arc<tokio::sync::RwLock<Vec<N>>>,
}
