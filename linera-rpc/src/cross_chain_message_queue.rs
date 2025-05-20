// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and functions common to the gRPC and simple implementations.

#![cfg(with_server)]

use std::{
    collections::{hash_map::Entry, HashMap},
    future::Future,
    time::Duration,
};

use futures::{channel::mpsc, StreamExt as _};
use linera_base::identifiers::ChainId;
use linera_core::data_types::CrossChainRequest;
use rand::Rng as _;
use tracing::{trace, warn};

use crate::config::ShardId;

#[cfg(with_metrics)]
static CROSS_CHAIN_MESSAGE_TASKS: std::sync::LazyLock<prometheus::IntGauge> =
    std::sync::LazyLock::new(|| {
        prometheus::register_int_gauge!(
            "cross_chain_message_tasks",
            "Number of concurrent cross-chain message tasks",
        )
        .expect("IntGauge can be created")
    });

#[expect(clippy::too_many_arguments)]
pub(crate) async fn forward_cross_chain_queries<F, G>(
    nickname: String,
    cross_chain_max_retries: u32,
    cross_chain_retry_delay: Duration,
    cross_chain_sender_delay: Duration,
    cross_chain_sender_failure_rate: f32,
    this_shard: ShardId,
    mut receiver: mpsc::Receiver<(CrossChainRequest, ShardId)>,
    handle_request: F,
) where
    F: Fn(ShardId, CrossChainRequest) -> G + Send + Clone + 'static,
    G: Future<Output = anyhow::Result<()>>,
{
    let mut futures = futures::stream::FuturesUnordered::new();
    let mut job_states: HashMap<QueueId, Job> = HashMap::new();

    let run_task = |task: Task| async move { handle_request(task.shard_id, task.request).await };

    let run_action = |action, queue, state: Job| async move {
        linera_base::time::timer::sleep(cross_chain_sender_delay).await;

        let to_shard = state.task.shard_id;

        (
            queue,
            match action {
                Action::Proceed { .. } => {
                    if let Err(error) = run_task(state.task).await {
                        warn!(
                            nickname = state.nickname,
                            %error,
                            retry = state.retries,
                            from_shard = this_shard,
                            to_shard,
                            "Failed to send cross-chain query",
                        );

                        Action::Retry
                    } else {
                        trace!(from_shard = this_shard, to_shard, "Sent cross-chain query",);

                        Action::Proceed {
                            id: state.id.wrapping_add(1),
                        }
                    }
                }

                Action::Retry => {
                    linera_base::time::timer::sleep(cross_chain_retry_delay * state.retries).await;
                    Action::Proceed { id: state.id }
                }
            },
        )
    };

    loop {
        tokio::select! {
            Some((queue, action)) = futures.next() => {
                let Entry::Occupied(mut state) = job_states.entry(queue) else {
                    panic!("running job without state");
                };

                let remove = matches!(action, Action::Proceed { id } if state.get().id < id)
                    || matches!(action, Action::Retry if state.get().retries >= cross_chain_max_retries);
                if remove {
                    state.remove();
                    continue;
                }

                if let Action::Retry = action {
                    state.get_mut().retries += 1
                }

                futures.push(run_action.clone()(action, queue, state.get().clone()));
            }

            request = receiver.next() => {
                let Some((request, shard_id)) = request else { break };
                #[cfg(with_metrics)]
                CROSS_CHAIN_MESSAGE_TASKS.set(job_states.len() as i64);

                if rand::thread_rng().gen::<f32>() < cross_chain_sender_failure_rate {
                    warn!("Dropped 1 cross-chain message intentionally.");
                    continue;
                }

                let queue = QueueId::new(&request);

                let task = Task {
                    shard_id,
                    request,
                };

                match job_states.entry(queue) {
                    Entry::Vacant(entry) => futures.push(run_action.clone()(
                        Action::Proceed { id: 0 },
                        queue,
                        entry.insert(Job {
                            id: 0,
                            retries: 0,
                            nickname: nickname.clone(),
                            task,
                        }).clone(),
                    )),

                    Entry::Occupied(mut entry) => {
                        entry.insert(Job {
                            id: entry.get().id + 1,
                            retries: 0,
                            nickname: nickname.clone(),
                            task,
                        });
                    }
                }
            }

            else => (),
        }
    }
}

/// An discriminant for message queues: messages with the same queue ID will be delivered
/// in order.
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
struct QueueId {
    sender: ChainId,
    recipient: ChainId,
    is_update: bool,
}

impl QueueId {
    /// Returns a discriminant for the message's queue.
    fn new(request: &CrossChainRequest) -> Self {
        let (sender, recipient, is_update) = match request {
            CrossChainRequest::UpdateRecipient {
                sender, recipient, ..
            } => (*sender, *recipient, true),
            CrossChainRequest::ConfirmUpdatedRecipient {
                sender, recipient, ..
            } => (*sender, *recipient, false),
        };
        QueueId {
            sender,
            recipient,
            is_update,
        }
    }
}

enum Action {
    Proceed { id: usize },
    Retry,
}

#[derive(Clone)]
struct Task {
    pub shard_id: ShardId,
    pub request: linera_core::data_types::CrossChainRequest,
}

#[derive(Clone)]
struct Job {
    pub id: usize,
    pub retries: u32,
    pub nickname: String,
    pub task: Task,
}
