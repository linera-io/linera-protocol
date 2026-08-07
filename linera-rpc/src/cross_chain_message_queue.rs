// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and functions common to the gRPC and simple implementations.

#![cfg(with_server)]

use std::{
    collections::{hash_map::Entry, HashMap},
    future::Future,
    panic::AssertUnwindSafe,
    time::Duration,
};

use futures::{channel::mpsc, FutureExt as _, StreamExt as _};
use linera_base::identifiers::ChainId;
#[cfg(with_metrics)]
use linera_base::time::Instant;
use linera_core::data_types::CrossChainRequest;
use rand::Rng as _;
use tracing::{error, trace, warn};

use crate::config::ShardId;

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        exponential_bucket_latencies, register_histogram, register_int_gauge,
    };
    use prometheus::{Histogram, IntGauge};

    pub static CROSS_CHAIN_MESSAGE_TASKS: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge(
            "cross_chain_message_tasks",
            "Number of concurrent cross-chain message tasks",
        )
    });

    pub static CROSS_CHAIN_QUEUE_WAIT_TIME: LazyLock<Histogram> = LazyLock::new(|| {
        register_histogram(
            "cross_chain_queue_wait_time",
            "Time (ms) a cross-chain message waits in queue before handle_request is called",
            exponential_bucket_latencies(10_000.0),
        )
    });
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn forward_cross_chain_queries<F, G>(
    nickname: String,
    cross_chain_max_retries: u32,
    cross_chain_retry_delay: Duration,
    cross_chain_max_backoff: Duration,
    cross_chain_sender_delay: Duration,
    cross_chain_sender_failure_rate: f32,
    this_shard: ShardId,
    mut receiver: mpsc::Receiver<(CrossChainRequest, ShardId)>,
    handle_request: F,
) where
    F: Fn(ShardId, CrossChainRequest) -> G + Send + Clone + 'static,
    G: Future<Output = anyhow::Result<()>>,
{
    let mut steps = futures::stream::FuturesUnordered::new();
    let mut job_states: HashMap<QueueId, JobState> = HashMap::new();

    let run_task = |task: Task| async move {
        // Record how long the message waited in queue (in milliseconds)
        #[cfg(with_metrics)]
        {
            let queue_wait_time_ms = task.queued_at.elapsed().as_secs_f64() * 1000.0;
            metrics::CROSS_CHAIN_QUEUE_WAIT_TIME.observe(queue_wait_time_ms);
        }

        handle_request(task.shard_id, task.request).await
    };

    let run_action = |action, queue, state: JobState| async move {
        linera_base::time::timer::sleep(cross_chain_sender_delay).await;

        let to_shard = state.task.shard_id;

        (
            queue,
            match action {
                Action::Proceed { .. } => {
                    let target_chain_id = state.task.request.target_chain_id();
                    if let Err(error) = run_task(state.task).await {
                        warn!(
                            nickname = state.nickname,
                            ?error,
                            retry = state.retries,
                            from_shard = this_shard,
                            to_shard,
                            chain_id = %target_chain_id,
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
                    let delay = cross_chain_retry_delay
                        .saturating_mul(state.retries)
                        .min(cross_chain_max_backoff);
                    linera_base::time::timer::sleep(delay).await;
                    Action::Proceed { id: state.id }
                }
            },
        )
    };

    // Every step must report back the queue it belongs to. A step that ends in a panic
    // reports nothing: `join_next` yields a `JoinError` instead of a `(QueueId, Action)`
    // pair, so the queue keeps its entry in `job_states` with no running step, and every
    // later request for it is only recorded there and never sent — that queue stops
    // delivering for the lifetime of the process. Catching the panic turns it into an
    // ordinary failed attempt, leaving the queue under the usual retry and give-up logic.
    let run_action = move |action, queue: QueueId, state| {
        let step = run_action.clone()(action, queue, state);
        async move {
            AssertUnwindSafe(step)
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    error!(
                        from_shard = this_shard,
                        sender = %queue.sender,
                        recipient = %queue.recipient,
                        "Panic while sending a cross-chain query; treating it as a failed attempt",
                    );
                    (queue, Action::Retry)
                })
        }
    };

    loop {
        #[cfg(with_metrics)]
        metrics::CROSS_CHAIN_MESSAGE_TASKS.set(job_states.len() as i64);

        tokio::select! {
            Some((queue, action)) = steps.next() => {
                let Entry::Occupied(mut state) = job_states.entry(queue) else {
                    panic!("running job without state");
                };

                if state.get().is_finished(&action, cross_chain_max_retries) {
                    state.remove();
                    continue;
                }

                if let Action::Retry = action {
                    state.get_mut().retries += 1
                }

                steps.push(run_action.clone()(action, queue, state.get().clone()));
            }

            request = receiver.next() => {
                let Some((request, shard_id)) = request else { break };

                if rand::thread_rng().gen::<f32>() < cross_chain_sender_failure_rate {
                    warn!("Dropped 1 cross-chain message intentionally.");
                    continue;
                }

                let queue = QueueId::new(&request);

                let task = Task {
                    shard_id,
                    request,
                    #[cfg(with_metrics)]
                    queued_at: Instant::now(),
                };

                match job_states.entry(queue) {
                    Entry::Vacant(entry) => steps.push(run_action.clone()(
                        Action::Proceed { id: 0 },
                        queue,
                        entry.insert(JobState {
                            id: 0,
                            retries: 0,
                            nickname: nickname.clone(),
                            task,
                        }).clone(),
                    )),

                    Entry::Occupied(mut entry) => {
                        entry.insert(JobState {
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
            }
            | CrossChainRequest::RevertConfirm {
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
    /// The request has been sent successfully and the next request can be sent.
    Proceed { id: usize },
    /// The request failed and should be retried.
    Retry,
}

#[derive(Clone)]
struct Task {
    /// The ID of the shard the request is sent to.
    pub shard_id: ShardId,
    /// The cross-chain request to be sent.
    pub request: linera_core::data_types::CrossChainRequest,
    /// When this task was queued.
    #[cfg(with_metrics)]
    pub queued_at: Instant,
}

#[derive(Clone)]
struct JobState {
    /// Queued requests are assigned incremental IDs.
    pub id: usize,
    /// How often the current request has been retried.
    pub retries: u32,
    /// The nickname of this worker, i.e. the one that is sending the request.
    pub nickname: String,
    /// The current request to be sent.
    pub task: Task,
}

impl JobState {
    /// Returns whether the job is finished and should be removed.
    fn is_finished(&self, action: &Action, max_retries: u32) -> bool {
        match action {
            // If the action is to proceed and no new messages with a higher ID are waiting.
            Action::Proceed { id } => self.id < *id,
            // If the action is to retry and the maximum number of retries has been reached.
            Action::Retry => self.retries >= max_retries,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

    use futures::{future::BoxFuture, SinkExt as _};
    use linera_base::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};
    use tokio::{
        sync::{
            mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
            Mutex as AsyncMutex, Semaphore,
        },
        task::JoinHandle,
    };

    use super::*;

    const NO_DELAY: Duration = Duration::ZERO;
    const NEVER_DROP: f32 = 0.0;
    const ALWAYS_DROP: f32 = 1.0;
    const THIS_SHARD: ShardId = 7;

    /// How long [`settle`] waits. Every test runs with a paused clock, so this bounds the
    /// amount of *simulated* time a test is willing to skip, not a wall-clock delay.
    const SETTLE: Duration = Duration::from_secs(60);

    type RequestSender = mpsc::Sender<(CrossChainRequest, ShardId)>;

    /// Lets the forwarder run until it has no more progress to make, firing any retry
    /// backoff shorter than [`SETTLE`] along the way.
    async fn settle() {
        linera_base::time::timer::sleep(SETTLE).await;
    }

    fn chain(index: u8) -> ChainId {
        ChainId(CryptoHash::test_hash(format!("chain {index}")))
    }

    /// A confirmation request. `tag` is carried in `latest_height` so that tests can tell
    /// the requests on one queue apart.
    fn confirm(sender: u8, recipient: u8, tag: u64) -> CrossChainRequest {
        CrossChainRequest::ConfirmUpdatedRecipient {
            sender: chain(sender),
            recipient: chain(recipient),
            latest_height: BlockHeight(tag),
        }
    }

    fn update(sender: u8, recipient: u8) -> CrossChainRequest {
        CrossChainRequest::UpdateRecipient {
            sender: chain(sender),
            recipient: chain(recipient),
            bundles: Vec::new(),
            previous_height: None,
        }
    }

    /// A `handle_request` implementation that records the requests it is given and decides
    /// their outcome.
    #[derive(Clone)]
    struct Handler {
        calls: Arc<Mutex<Vec<(ShardId, CrossChainRequest)>>>,
        /// How many further calls fail before they start succeeding.
        failures_left: Arc<AtomicUsize>,
        /// How many further calls panic before they stop panicking.
        panics_left: Arc<AtomicUsize>,
        /// If set, each call takes one permit before returning, so that a test can hold
        /// requests in flight.
        gate: Option<Arc<Semaphore>>,
        /// Emits one message per call, so that [`Handler::wait_for_calls`] can await
        /// instead of spinning.
        call_signal: UnboundedSender<()>,
        call_signals: Arc<AsyncMutex<UnboundedReceiver<()>>>,
    }

    impl Handler {
        fn new() -> Self {
            let (call_signal, call_signals) = unbounded_channel();
            Handler {
                calls: Arc::default(),
                failures_left: Arc::new(AtomicUsize::new(0)),
                panics_left: Arc::new(AtomicUsize::new(0)),
                gate: None,
                call_signal,
                call_signals: Arc::new(AsyncMutex::new(call_signals)),
            }
        }

        /// Makes the next `count` calls fail.
        fn failing(self, count: usize) -> Self {
            self.failures_left.store(count, Ordering::SeqCst);
            self
        }

        /// Makes the next `count` calls panic.
        fn panicking(self, count: usize) -> Self {
            self.panics_left.store(count, Ordering::SeqCst);
            self
        }

        /// Makes every call block until the test releases it with [`Handler::release`].
        fn gated(mut self) -> Self {
            self.gate = Some(Arc::new(Semaphore::new(0)));
            self
        }

        /// Lets `count` blocked calls return.
        fn release(&self, count: usize) {
            self.gate
                .as_ref()
                .expect("handler is gated")
                .add_permits(count);
        }

        /// The closure to hand to [`forward_cross_chain_queries`].
        fn as_fn(
            &self,
        ) -> impl Fn(ShardId, CrossChainRequest) -> BoxFuture<'static, anyhow::Result<()>>
               + Send
               + Clone
               + 'static {
            let this = self.clone();
            move |shard_id, request| {
                let this = this.clone();
                Box::pin(async move { this.call(shard_id, request).await })
            }
        }

        async fn call(self, shard_id: ShardId, request: CrossChainRequest) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push((shard_id, request));
            // A closed receiver just means the test has finished waiting for calls.
            self.call_signal.send(()).ok();
            if let Some(gate) = &self.gate {
                gate.acquire()
                    .await
                    .expect("the gate is never closed")
                    .forget();
            }
            // `checked_sub` yields `None` once a budget is exhausted, which makes
            // `fetch_update` fail without touching the counter.
            let take = |budget: &AtomicUsize| {
                budget
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                        left.checked_sub(1)
                    })
                    .is_ok()
            };
            if take(&self.panics_left) {
                panic!("simulated transport panic");
            }
            if take(&self.failures_left) {
                anyhow::bail!("simulated transport failure");
            }
            Ok(())
        }

        /// Waits until `handle_request` has been called `count` times in total, panicking if
        /// that many calls never arrive.
        ///
        /// Awaiting rather than polling in a loop is what makes the timing assertions work:
        /// the paused clock only advances while every task is idle, so a spinning test would
        /// stop time and hang. The [`SETTLE`] bound is simulated time too, so a forwarder
        /// that stops making calls fails the test instead of hanging it.
        async fn wait_for_calls(&self, count: usize) {
            let wait = async {
                let mut signals = self.call_signals.lock().await;
                while self.call_count() < count {
                    signals.recv().await.expect("the handler outlives the test");
                }
            };
            linera_base::time::timer::timeout(SETTLE, wait)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "expected {count} calls, but the forwarder stopped after {}",
                        self.call_count()
                    )
                });
        }

        fn calls(&self) -> Vec<(ShardId, CrossChainRequest)> {
            self.calls.lock().unwrap().clone()
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }

        /// The `tag` of each delivered [`confirm`] request, in delivery order.
        fn tags(&self) -> Vec<u64> {
            self.calls()
                .into_iter()
                .map(|(_, request)| match request {
                    CrossChainRequest::ConfirmUpdatedRecipient { latest_height, .. } => {
                        latest_height.0
                    }
                    other => panic!("not a confirmation: {other:?}"),
                })
                .collect()
        }
    }

    /// Spawns a forwarder and returns the sender feeding it, plus its join handle.
    fn spawn_forwarder(
        handler: &Handler,
        max_retries: u32,
        retry_delay: Duration,
        max_backoff: Duration,
        sender_delay: Duration,
        failure_rate: f32,
    ) -> (RequestSender, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(100);
        let task = tokio::spawn(forward_cross_chain_queries(
            "test".to_string(),
            max_retries,
            retry_delay,
            max_backoff,
            sender_delay,
            failure_rate,
            THIS_SHARD,
            receiver,
            handler.as_fn(),
        ));
        (sender, task)
    }

    /// Spawns a forwarder that never retries, never delays and never drops.
    fn spawn_simple_forwarder(handler: &Handler) -> (RequestSender, JoinHandle<()>) {
        spawn_forwarder(handler, 0, NO_DELAY, NO_DELAY, NO_DELAY, NEVER_DROP)
    }

    async fn send(sender: &mut RequestSender, request: CrossChainRequest, shard_id: ShardId) {
        sender
            .send((request, shard_id))
            .await
            .expect("the forwarder is running");
    }

    #[tokio::test(start_paused = true)]
    async fn test_forwards_a_request_to_its_target_shard() {
        let handler = Handler::new();
        let (mut sender, _task) = spawn_simple_forwarder(&handler);

        send(&mut sender, confirm(1, 2, 5), 3).await;
        settle().await;

        assert_eq!(handler.calls(), vec![(3, confirm(1, 2, 5))]);
    }

    /// Requests on one queue are delivered in the order they were submitted.
    #[tokio::test(start_paused = true)]
    async fn test_preserves_order_within_a_queue() {
        let handler = Handler::new();
        let (mut sender, _task) = spawn_simple_forwarder(&handler);

        for tag in 1..=3 {
            send(&mut sender, confirm(1, 2, tag), 0).await;
            settle().await;
        }

        assert_eq!(handler.tags(), vec![1, 2, 3]);
    }

    /// A queue holds at most one pending request: while one is in flight, further requests
    /// for the same queue overwrite each other and only the last one is delivered. Senders
    /// must therefore not rely on every request they submit reaching its target.
    #[tokio::test(start_paused = true)]
    async fn test_coalesces_requests_queued_behind_an_in_flight_one() {
        let handler = Handler::new().gated();
        let (mut sender, _task) = spawn_simple_forwarder(&handler);

        send(&mut sender, confirm(1, 2, 1), 0).await;
        settle().await;
        assert_eq!(handler.tags(), vec![1], "the first request is in flight");

        send(&mut sender, confirm(1, 2, 2), 0).await;
        send(&mut sender, confirm(1, 2, 3), 0).await;
        settle().await;
        assert_eq!(handler.tags(), vec![1], "nothing else has started yet");

        handler.release(1);
        settle().await;
        assert_eq!(handler.tags(), vec![1, 3], "request 2 was superseded by 3");
    }

    /// Queues are keyed by sender and recipient, so a request stuck in flight for one
    /// recipient does not hold up another.
    #[tokio::test(start_paused = true)]
    async fn test_queues_for_different_recipients_are_independent() {
        let handler = Handler::new().gated();
        let (mut sender, _task) = spawn_simple_forwarder(&handler);

        send(&mut sender, confirm(1, 2, 1), 0).await;
        send(&mut sender, confirm(1, 3, 2), 0).await;
        settle().await;

        let mut tags = handler.tags();
        tags.sort_unstable();
        assert_eq!(tags, vec![1, 2], "both queues started concurrently");
    }

    /// The queue discriminant also covers whether a request is an update, so updates and
    /// confirmations between the same pair of chains do not block each other.
    #[tokio::test(start_paused = true)]
    async fn test_updates_and_confirmations_are_separate_queues() {
        let handler = Handler::new().gated();
        let (mut sender, _task) = spawn_simple_forwarder(&handler);

        send(&mut sender, update(1, 2), 0).await;
        send(&mut sender, confirm(1, 2, 9), 0).await;
        settle().await;

        assert_eq!(
            handler.call_count(),
            2,
            "the update did not hold up the confirmation",
        );
    }

    /// A failing request is retried until it succeeds.
    #[tokio::test(start_paused = true)]
    async fn test_retries_until_the_request_succeeds() {
        let handler = Handler::new().failing(2);
        let (mut sender, _task) = spawn_forwarder(
            &handler,
            5,
            Duration::from_secs(1),
            Duration::from_secs(30),
            NO_DELAY,
            NEVER_DROP,
        );

        send(&mut sender, confirm(1, 2, 1), 0).await;
        settle().await;

        assert_eq!(handler.tags(), vec![1, 1, 1], "two failures, then success");
    }

    /// After `max_retries` retries the request is given up on, having been attempted
    /// `max_retries + 1` times in total.
    #[tokio::test(start_paused = true)]
    async fn test_gives_up_after_max_retries() {
        let handler = Handler::new().failing(usize::MAX);
        let (mut sender, _task) = spawn_forwarder(
            &handler,
            2,
            Duration::from_secs(1),
            Duration::from_secs(30),
            NO_DELAY,
            NEVER_DROP,
        );

        send(&mut sender, confirm(1, 2, 1), 0).await;
        settle().await;

        assert_eq!(handler.call_count(), 3, "one attempt plus two retries");
    }

    #[tokio::test(start_paused = true)]
    async fn test_does_not_retry_when_max_retries_is_zero() {
        let handler = Handler::new().failing(usize::MAX);
        let (mut sender, _task) = spawn_simple_forwarder(&handler);

        send(&mut sender, confirm(1, 2, 1), 0).await;
        settle().await;

        assert_eq!(handler.call_count(), 1);
    }

    /// Giving up on a request releases its queue, so a later request on that queue is still
    /// delivered rather than being stuck behind the abandoned one.
    #[tokio::test(start_paused = true)]
    async fn test_queue_recovers_after_giving_up_on_a_request() {
        let handler = Handler::new().failing(3);
        let (mut sender, _task) = spawn_forwarder(
            &handler,
            2,
            Duration::from_secs(1),
            Duration::from_secs(30),
            NO_DELAY,
            NEVER_DROP,
        );

        send(&mut sender, confirm(1, 2, 1), 0).await;
        settle().await;
        assert_eq!(
            handler.tags(),
            vec![1, 1, 1],
            "gave up on the first request",
        );

        send(&mut sender, confirm(1, 2, 2), 0).await;
        settle().await;
        assert_eq!(
            handler.tags(),
            vec![1, 1, 1, 2],
            "the queue is usable again",
        );
    }

    /// A panicking transport is treated like a failing one: the request is retried rather
    /// than lost, and the forwarder keeps running.
    #[tokio::test(start_paused = true)]
    async fn test_retries_after_a_panicking_transport() {
        let handler = Handler::new().panicking(1);
        let (mut sender, _task) = spawn_forwarder(
            &handler,
            5,
            Duration::from_secs(1),
            Duration::from_secs(30),
            NO_DELAY,
            NEVER_DROP,
        );

        send(&mut sender, confirm(1, 2, 1), 0).await;
        settle().await;

        assert_eq!(handler.tags(), vec![1, 1], "one panic, then success");
    }

    /// Giving up on a request that kept panicking still releases its queue. Without that, the
    /// queue would keep an entry with no running step, and every later request for it would
    /// be recorded there and never sent.
    #[tokio::test(start_paused = true)]
    async fn test_queue_recovers_after_giving_up_on_a_panicking_request() {
        let handler = Handler::new().panicking(2);
        let (mut sender, _task) = spawn_forwarder(
            &handler,
            1,
            Duration::from_secs(1),
            Duration::from_secs(30),
            NO_DELAY,
            NEVER_DROP,
        );

        send(&mut sender, confirm(1, 2, 1), 0).await;
        settle().await;
        assert_eq!(handler.tags(), vec![1, 1], "one attempt plus one retry");

        send(&mut sender, confirm(1, 2, 2), 0).await;
        settle().await;
        assert_eq!(handler.tags(), vec![1, 1, 2], "the queue is usable again");
    }

    /// The wait before the n-th retry is `retry_delay * n`, capped at `max_backoff`.
    #[tokio::test(start_paused = true)]
    async fn test_retry_delay_grows_linearly_up_to_the_maximum() {
        let handler = Handler::new().failing(usize::MAX);
        let (mut sender, _task) = spawn_forwarder(
            &handler,
            4,
            Duration::from_secs(10),
            Duration::from_secs(25),
            NO_DELAY,
            NEVER_DROP,
        );

        let start = tokio::time::Instant::now();
        send(&mut sender, confirm(1, 2, 1), 0).await;

        let mut attempt_times = Vec::new();
        for attempt in 1..=5 {
            handler.wait_for_calls(attempt).await;
            attempt_times.push(start.elapsed());
        }
        settle().await;

        assert_eq!(handler.call_count(), 5, "one attempt plus four retries");
        // Waits of 10s, 20s, 25s (capped) and 25s between consecutive attempts.
        assert_eq!(
            attempt_times,
            vec![
                Duration::ZERO,
                Duration::from_secs(10),
                Duration::from_secs(30),
                Duration::from_secs(55),
                Duration::from_secs(80),
            ],
        );
    }

    /// Every request waits `sender_delay` before it is handed to the transport.
    #[tokio::test(start_paused = true)]
    async fn test_sender_delay_postpones_every_request() {
        let sender_delay = Duration::from_secs(5);
        let handler = Handler::new();
        let (mut sender, _task) =
            spawn_forwarder(&handler, 0, NO_DELAY, NO_DELAY, sender_delay, NEVER_DROP);

        let start = tokio::time::Instant::now();
        send(&mut sender, confirm(1, 2, 1), 0).await;
        handler.wait_for_calls(1).await;

        assert_eq!(start.elapsed(), sender_delay);
    }

    /// The failure rate drops requests before they reach the transport, and dropped requests
    /// are not retried.
    #[tokio::test(start_paused = true)]
    async fn test_failure_rate_of_one_drops_every_request() {
        let handler = Handler::new();
        let (mut sender, _task) =
            spawn_forwarder(&handler, 10, NO_DELAY, NO_DELAY, NO_DELAY, ALWAYS_DROP);

        for tag in 1..=5 {
            send(&mut sender, confirm(1, 2, tag), 0).await;
        }
        settle().await;

        assert_eq!(handler.call_count(), 0);
    }

    /// Closing the channel ends the forwarder.
    #[tokio::test(start_paused = true)]
    async fn test_closing_the_channel_stops_the_forwarder() {
        let handler = Handler::new();
        let (mut sender, task) = spawn_simple_forwarder(&handler);

        send(&mut sender, confirm(1, 2, 1), 0).await;
        settle().await;
        drop(sender);

        linera_base::time::timer::timeout(SETTLE, task)
            .await
            .expect("the forwarder stops once the channel is closed")
            .expect("the forwarder does not panic");
    }
}
