// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use linera_base::{data_types::Timestamp, time::Duration};
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
///
/// The entries map is guarded by a synchronous [`Mutex`] (never held across an `.await`)
/// so that an owner's [`InFlightGuard`] can remove its entry from `Drop`, which is what
/// makes deduplication cancel-safe — see [`InFlightTracker::insert_new`].
#[derive(Debug, Clone)]
pub(super) struct InFlightTracker<N> {
    /// Maps request keys to in-flight entries containing broadcast senders and metadata
    entries: Arc<Mutex<HashMap<RequestKey, InFlightEntry<N>>>>,
    /// Source of per-entry generation numbers, so an owner only removes the entry it
    /// created and not a later owner that reused the same key.
    next_generation: Arc<AtomicU64>,
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
            entries: Arc::new(Mutex::new(HashMap::new())),
            next_generation: Arc::new(AtomicU64::new(0)),
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
    pub(super) fn try_subscribe(&self, key: &RequestKey, now: Timestamp) -> Option<InFlightMatch> {
        let in_flight = self
            .entries
            .lock()
            .expect("in-flight tracker mutex poisoned");

        if let Some(entry) = in_flight.get(key) {
            let elapsed = now.duration_since(entry.started_at);

            if elapsed <= self.timeout {
                return Some(InFlightMatch::Exact(Subscribed(entry.sender.subscribe())));
            }
        }

        // Sometimes a request key may not have the exact match but may be subsumed by a larger one.
        for (in_flight_key, entry) in in_flight.iter() {
            if in_flight_key.subsumes(key) {
                let elapsed = now.duration_since(entry.started_at);

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

    /// Inserts a new in-flight request entry and returns the owner guard.
    ///
    /// The returned [`InFlightGuard`] owns the entry. Completing the request via
    /// [`InFlightGuard::complete_and_broadcast`] broadcasts the result to all
    /// subscribers and removes the entry. If the guard is instead dropped without
    /// completing — for example because the owning task was cancelled, such as a
    /// losing branch of `communicate_with_quorum` — its `Drop` removes the entry,
    /// dropping the broadcast sender so subscribers wake with `RecvError::Closed`
    /// and execute the request themselves rather than waiting forever.
    ///
    /// # Arguments
    /// - `key`: The request key to insert
    /// - `now`: The current time, recorded as the entry's start time
    pub(super) fn insert_new(&self, key: RequestKey, now: Timestamp) -> InFlightGuard<N> {
        let (sender, _receiver) = broadcast::channel(1);
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        self.entries
            .lock()
            .expect("in-flight tracker mutex poisoned")
            .insert(
                key.clone(),
                InFlightEntry {
                    sender,
                    started_at: now,
                    generation,
                    alternative_peers: Arc::new(tokio::sync::RwLock::new(Vec::new())),
                },
            );
        InFlightGuard {
            entries: self.entries.clone(),
            key,
            generation,
        }
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
        let Some(alternative_peers) = self.alternative_peers(key) else {
            return;
        };
        let mut alt_peers = alternative_peers.write().await;
        if !alt_peers.contains(&peer) {
            alt_peers.push(peer);
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
        let alternative_peers = self.alternative_peers(key)?;
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
        let Some(alternative_peers) = self.alternative_peers(key) else {
            return;
        };
        alternative_peers.write().await.retain(|p| p != peer);
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
        self.alternative_peers(key)?.write().await.pop()
    }

    /// Returns the shared alternative-peers list for an entry, if it exists.
    ///
    /// The entries lock is released before the caller awaits the returned lock, so the
    /// synchronous [`Mutex`] is never held across an `.await`.
    fn alternative_peers(&self, key: &RequestKey) -> Option<Arc<tokio::sync::RwLock<Vec<N>>>> {
        self.entries
            .lock()
            .expect("in-flight tracker mutex poisoned")
            .get(key)
            .map(|entry| entry.alternative_peers.clone())
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

/// Owns an in-flight entry for the lifetime of the request that created it.
///
/// Completing via [`InFlightGuard::complete_and_broadcast`] is the success path. If the
/// guard is dropped first (the owning task was cancelled), `Drop` removes the entry so
/// subscribers stop waiting. Both paths are generation-checked, so a guard never touches
/// an entry a later owner created after this one went stale and was replaced.
pub(super) struct InFlightGuard<N> {
    entries: Arc<Mutex<HashMap<RequestKey, InFlightEntry<N>>>>,
    key: RequestKey,
    generation: u64,
}

impl<N> InFlightGuard<N> {
    /// Removes the owned entry and broadcasts the result to all waiting subscribers.
    ///
    /// Returns the number of subscribers that were notified. Does nothing (returns `0`)
    /// if a later owner has already replaced this entry, in which case this result is
    /// stale and must not be broadcast. After this call the guard's `Drop` is a no-op.
    pub(super) fn complete_and_broadcast(
        &self,
        result: Arc<Result<RequestResult, NodeError>>,
    ) -> usize {
        let mut in_flight = self
            .entries
            .lock()
            .expect("in-flight tracker mutex poisoned");
        if in_flight
            .get(&self.key)
            .is_none_or(|entry| entry.generation != self.generation)
        {
            return 0;
        }
        let entry = in_flight.remove(&self.key).expect("checked above");
        let waiter_count = entry.sender.receiver_count();
        tracing::trace!(
            key = ?self.key,
            waiters = waiter_count,
            "request completed; broadcasting result to waiters",
        );
        if waiter_count != 0 {
            if let Err(err) = entry.sender.send(result) {
                tracing::warn!(
                    key = ?self.key,
                    error = ?err,
                    "failed to broadcast result to waiters"
                );
            }
        }
        waiter_count
    }
}

impl<N> Drop for InFlightGuard<N> {
    fn drop(&mut self) {
        let Ok(mut in_flight) = self.entries.lock() else {
            return; // Poisoned mutex: nothing safe to do from `Drop`.
        };
        // Remove only if our entry is still the current one for this key. Dropping it
        // drops the broadcast sender, waking subscribers with `Closed` so they execute
        // the request themselves instead of waiting for a result that will never arrive.
        if in_flight
            .get(&self.key)
            .is_some_and(|entry| entry.generation == self.generation)
        {
            in_flight.remove(&self.key);
        }
    }
}

/// In-flight request entry that tracks when the request was initiated.
#[derive(Debug)]
pub(super) struct InFlightEntry<N> {
    /// Broadcast sender for notifying waiters when the request completes
    sender: broadcast::Sender<Arc<Result<RequestResult, NodeError>>>,
    /// Time when this request was initiated
    started_at: Timestamp,
    /// Generation of this entry, used by the owner guard to remove only its own entry.
    generation: u64,
    /// Alternative peers that can provide this data if the primary request fails
    alternative_peers: Arc<tokio::sync::RwLock<Vec<N>>>,
}
