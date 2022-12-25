use super::ChainGuards;
use futures::FutureExt;
use linera_base::data_types::ChainId;
use std::{
    mem,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::Barrier, time::sleep};

/// Test if a released chain guard does not stay in memory.
#[tokio::test]
async fn dropped_guard_does_not_leak() {
    let chain_id = ChainId::root(0);
    let guards = ChainGuards::default();
    // Obtain the guard
    let guard = guards.guard(chain_id).await;
    assert_eq!(guards.active_guards(), 1);
    // Drop the guard
    mem::drop(guard);
    assert_eq!(guards.active_guards(), 0);
}

/// Test if a chain guard can be obtained again after it has been dropped.
#[tokio::test]
async fn guard_can_be_obtained_later_again() {
    let chain_id = ChainId::root(0);
    let guards = ChainGuards::default();
    // Obtain the guard the first time and drop it immediately
    let _ = guards.guard(chain_id).await;
    // It should be available immediately on the second time
    assert!(guards.guard(chain_id).now_or_never().is_some());
}

/// Test if two tasks obtaining a guard for the same chain obtain them sequentially.
#[tokio::test(start_paused = true)]
async fn prevents_concurrent_access_to_the_same_chain() {
    let chain_id = ChainId::root(0);

    let access = ConcurrentAccessTest::default()
        .spawn_two_tasks_to_obtain_guards_for(chain_id, chain_id)
        .await;

    assert_eq!(access, Access::Sequential);
}

/// Test if two tasks obtaining a guard for different chains obtain them concurrently.
#[tokio::test(start_paused = true)]
async fn allows_concurrent_access_to_different_chains() {
    let access = ConcurrentAccessTest::default()
        .spawn_two_tasks_to_obtain_guards_for(ChainId::root(0), ChainId::root(1))
        .await;

    assert_eq!(access, Access::Concurrent);
}

/// Test helper for running two tasks to obtain chain guards.
#[derive(Clone)]
pub struct ConcurrentAccessTest {
    guards: ChainGuards,
    after_first_guard_is_obtained: Arc<Barrier>,
    first_task_finished: Arc<AtomicBool>,
}

/// Result from [`ConcurrentAccessTest::spawn_two_tasks_to_obtain_guards_for`], indicating if the
/// locks were obtained concurrently or sequentially.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Access {
    Concurrent,
    Sequential,
}

impl Default for ConcurrentAccessTest {
    fn default() -> Self {
        ConcurrentAccessTest {
            guards: ChainGuards::default(),
            after_first_guard_is_obtained: Arc::new(Barrier::new(2)),
            first_task_finished: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl ConcurrentAccessTest {
    /// Spawn two tasks and check if they access `first_chain` and `second_chain` concurrently or
    /// sequentially.
    pub async fn spawn_two_tasks_to_obtain_guards_for(
        self,
        first_chain: ChainId,
        second_chain: ChainId,
    ) -> Access {
        let first_task = tokio::spawn(self.clone().run_first_task(first_chain));
        let second_task = tokio::spawn(self.run_second_task(second_chain));

        first_task.await.expect("First task failed");
        second_task.await.expect("Second task failed")
    }

    /// First concurrent task obtains a guard for the `chain_id` before the second task obtains its
    /// guard.
    ///
    /// After the guard is obtained it synchronizes on `after_first_guard_is_obtained`, sleeps for
    /// a while to ensure the other task runs as much as it can, then marks `first_task_finished`
    /// and drops the guard.
    async fn run_first_task(self, chain_id: ChainId) {
        let _guard = self.guards.guard(chain_id).await;
        self.after_first_guard_is_obtained.wait().await;

        sleep(Duration::from_secs(10)).await;

        self.first_task_finished.store(true, Ordering::Release);
    }

    /// Second concurrent tasks waits to try to obtain the guard only after the first task already
    /// has its guard.
    ///
    /// Waits until the first task acquires the lock, then immediately tries to acquire it. By the
    /// time it manages to acquire it, it will check if the first task has already finished to
    /// determine if the access was concurrent or sequential.
    async fn run_second_task(self, chain_id: ChainId) -> Access {
        self.after_first_guard_is_obtained.wait().await;
        let _guard = self.guards.guard(chain_id).await;

        match self.first_task_finished.load(Ordering::Acquire) {
            false => Access::Concurrent,
            true => Access::Sequential,
        }
    }
}
