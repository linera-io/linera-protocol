// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use futures::{
    channel::{mpsc, oneshot},
    future, SinkExt, StreamExt,
};
use std::collections::BTreeMap;

#[cfg(test)]
#[path = "unit_tests/downloader_tests.rs"]
mod downloader_tests;

/// An asynchronous downloader that ensures that the value for each key is requested at most once.
pub struct Downloader<R, K, V> {
    /// User-provided logics to fetch data.
    requester: R,
    /// Status of previous downloads, indexed by key.
    downloads: BTreeMap<K, DownloadStatus<V>>,
    /// Command stream of the main handler.
    command_receiver: mpsc::UnboundedReceiver<DownloadCommand<K, V>>,
    /// How to send commands to the main handler.
    command_sender: mpsc::UnboundedSender<DownloadCommand<K, V>>,
}

/// The underlying data-fetching mechanism to be provided by the user.
pub trait Requester {
    type Key: std::cmp::Ord + Send + Sync + Clone + 'static;
    type Value: std::fmt::Debug + Send + Clone + 'static;

    /// Request the value corresponding to the given key.
    fn query(&mut self, key: Self::Key) -> future::BoxFuture<Self::Value>;
}

/// Channel for using code to send requests and stop the downloader task.
#[derive(Clone)]
pub struct DownloadHandle<K, V>(mpsc::UnboundedSender<DownloadCommand<K, V>>);

/// A command send to the downloader task.
enum DownloadCommand<K, V> {
    /// A user requests a value.
    Request(K, oneshot::Sender<V>),
    /// A value has been downloaded.
    Publish(K, V),
    /// Shut down the main handler.
    Quit,
}

/// The status of a download job.
enum DownloadStatus<V> {
    /// A value is available.
    Ready(V),
    /// Download is in progress. Subscribers are waiting for the result.
    WaitingList(Vec<oneshot::Sender<V>>),
}

impl<K, V> DownloadHandle<K, V> {
    /// Allow to make new download queries and wait for the result.
    pub async fn query(&mut self, key: K) -> Result<V, failure::Error> {
        let (callback, receiver) = oneshot::channel();
        self.0.send(DownloadCommand::Request(key, callback)).await?;
        let value = receiver.await?;
        Ok(value)
    }

    /// Shut down the main handler.
    pub async fn stop(&mut self) -> Result<(), failure::Error> {
        self.0.send(DownloadCommand::Quit).await?;
        Ok(())
    }
}

impl<R, K, V> Downloader<R, K, V> {
    /// Recover the content of the downloading cache.
    fn finalize(self) -> impl Iterator<Item = V> {
        self.downloads.into_iter().filter_map(|(_, v)| match v {
            DownloadStatus::Ready(value) => Some(value),
            _ => None,
        })
    }
}

impl<R, K, V> Downloader<R, K, V>
where
    R: Requester<Key = K, Value = V> + Send + Clone + 'static,
    K: std::cmp::Ord + Send + Sync + Clone + 'static,
    V: std::fmt::Debug + Send + Clone + 'static,
{
    /// Create a downloader as a wrapper around the given `requester`.
    /// Fill the initial cache with some known values.
    pub fn start<I>(
        requester: R,
        known_values: I,
    ) -> (
        tokio::task::JoinHandle<impl Iterator<Item = V>>,
        DownloadHandle<K, V>,
    )
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let (command_sender, command_receiver) = mpsc::unbounded();
        let mut downloads = BTreeMap::new();
        for (key, value) in known_values {
            downloads.insert(key, DownloadStatus::Ready(value));
        }
        let mut downloader = Self {
            requester,
            downloads,
            command_receiver,
            command_sender: command_sender.clone(),
        };
        // Spawn a task for the main handler.
        let task = tokio::spawn(async move {
            downloader.run().await;
            downloader.finalize()
        });
        (task, DownloadHandle(command_sender))
    }

    /// Main handler.
    async fn run(&mut self) {
        loop {
            match self.command_receiver.next().await {
                Some(DownloadCommand::Request(key, callback)) => {
                    // Deconstruct self to help the borrow checker below.
                    let requester_ref = &self.requester;
                    let command_sender_ref = &self.command_sender;
                    let downloads_ref_mut = &mut self.downloads;
                    // Recover current download status or create a new one.
                    let entry = downloads_ref_mut.entry(key.clone()).or_insert_with(|| {
                        let mut requester = requester_ref.clone();
                        let mut command_sender = command_sender_ref.clone();
                        tokio::spawn(async move {
                            let result = requester.query(key.clone()).await;
                            command_sender
                                .send(DownloadCommand::Publish(key, result))
                                .await
                                .unwrap_or(())
                        });
                        DownloadStatus::WaitingList(Vec::new())
                    });
                    // Process Request: either subscribe or return the result now.
                    match entry {
                        DownloadStatus::WaitingList(list) => {
                            list.push(callback);
                        }
                        DownloadStatus::Ready(result) => {
                            callback.send(result.clone()).unwrap_or(());
                        }
                    }
                }
                Some(DownloadCommand::Publish(key, result)) => {
                    // Handle newly found result.
                    let status = std::mem::replace(
                        self.downloads.get_mut(&key).expect("Key should be present"),
                        DownloadStatus::Ready(result.clone()),
                    );
                    if let DownloadStatus::WaitingList(subscribers) = status {
                        for callback in subscribers {
                            callback.send(result.clone()).unwrap_or(());
                        }
                    }
                }
                _ => return,
            }
        }
    }
}
