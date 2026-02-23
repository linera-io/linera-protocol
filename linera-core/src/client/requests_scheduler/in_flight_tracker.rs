// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, fmt::Debug, sync::Arc};

use linera_base::time::{Duration, Instant};
use tokio::sync::broadcast;

use super::{
    cache::SubsumingKey,
    request::{RequestKey, RequestResult},
};
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
    /// - `None`: No matching in-flight request found. Also returned if the found request is stale (exceeds timeout).
    /// - `Some(InFlightMatch::Subsuming { key, outcome })`: Subsuming request found
    pub(super) async fn try_subscribe(&self, key: &RequestKey) -> Option<InFlightMatch> {
        let in_flight = self.entries.read().await;

        if let Some(entry) = in_flight.get(key) {
            let elapsed = Instant::now().duration_since(entry.started_at);

            if elapsed <= self.timeout {
                return Some(InFlightMatch::Exact(Subscribed(entry.sender.subscribe())));
            }
        }

        // Sometimes a request key may not have the exact match but may be subsumed by a larger one.
        for (in_flight_key, entry) in in_flight.iter() {
            if in_flight_key.subsumes(key) {
                let elapsed = Instant::now().duration_since(entry.started_at);

                if elapsed <= self.timeout {
                    return Some(InFlightMatch::Subsuming {
                        key: in_flight_key.clone(),
                        outcome: Subscribed(entry.sender.subscribe()),
                    });
                }
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
            tracing::trace!(
                key = ?key,
                waiters = waiter_count,
                "request completed; broadcasting result to waiters",
            );
            if waiter_count != 0 {
                if let Err(err) = entry.sender.send(result) {
                    tracing::warn!(
                        key = ?key,
                        error = ?err,
                        "failed to broadcast result to waiters"
                    );
                }
            }
            return waiter_count;
        }
        0
    }

    async fn alternative_peers_lock(
        &self,
        key: &RequestKey,
    ) -> Option<Arc<tokio::sync::RwLock<Vec<N>>>> {
        let in_flight = self.entries.read().await;
        in_flight
            .get(key)
            .map(|entry| Arc::clone(&entry.alternative_peers))
    }

    /// Registers an alternative peer for an in-flight request.
    ///
    /// If an entry exists for the given key, registers the peer as an alternative source
    /// (if not already registered).
    ///
    /// # Arguments
    /// - `key`: The request key
    /// - `peer`: The peer to register as an alternative
    pub(super) async fn add_alternative_peer(&self, key: &RequestKey, peer: N)
    where
        N: PartialEq + Eq,
    {
        if let Some(alternative_peers) = self.alternative_peers_lock(key).await {
            // Register this peer as an alternative source if not already present.
            let mut alt_peers = alternative_peers.write().await;
            if !alt_peers.contains(&peer) {
                alt_peers.push(peer);
            }
        }
    }

    /// Retrieves the list of alternative peers registered for an in-flight request.
    ///
    /// Returns a clone of the alternative peers list if an entry exists for the given key.
    ///
    /// # Arguments
    /// - `key`: The request key to look up
    ///
    /// # Returns
    /// - `Vec<N>`: List of alternative peers (empty if no entry exists)
    pub(super) async fn get_alternative_peers(&self, key: &RequestKey) -> Option<Vec<N>> {
        let alternative_peers = self.alternative_peers_lock(key).await?;
        let peers = alternative_peers.read().await;
        Some(peers.clone())
    }

    /// Removes a specific peer from the alternative peers list.
    ///
    /// # Arguments
    /// - `key`: The request key to look up
    /// - `peer`: The peer to remove from alternatives
    pub(super) async fn remove_alternative_peer(&self, key: &RequestKey, peer: &N)
    where
        N: PartialEq + Eq,
    {
        if let Some(alternative_peers) = self.alternative_peers_lock(key).await {
            let mut alt_peers = alternative_peers.write().await;
            alt_peers.retain(|p| p != peer);
        }
    }

    /// Pops and returns the newest alternative peer from the list.
    ///
    /// Removes and returns the last peer from the alternative peers list (LIFO - newest first).
    /// Returns `None` if the entry doesn't exist or the list is empty.
    ///
    /// # Arguments
    /// - `key`: The request key to look up
    ///
    /// # Returns
    /// - `Some(N)`: The newest alternative peer
    /// - `None`: No entry exists or alternatives list is empty
    pub(super) async fn pop_alternative_peer(&self, key: &RequestKey) -> Option<N> {
        let alternative_peers = self.alternative_peers_lock(key).await?;
        let mut alt_peers = alternative_peers.write().await;
        alt_peers.pop()
    }
}

/// Type of in-flight request match found.
#[derive(Debug)]
pub(super) enum InFlightMatch {
    /// Exact key match found
    Exact(Subscribed),
    /// Subsuming key match found (larger request that contains this request)
    Subsuming {
        /// The key of the subsuming request
        key: RequestKey,
        /// Outcome of attempting to subscribe
        outcome: Subscribed,
    },
}

/// Outcome of attempting to subscribe to an in-flight request.
/// Successfully subscribed; receiver will be notified when request completes
#[derive(Debug)]
pub(super) struct Subscribed(pub(super) broadcast::Receiver<Arc<Result<RequestResult, NodeError>>>);

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
