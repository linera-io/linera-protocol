// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
Abstractions over time that can be used natively or on the Web.
 */

cfg_if::cfg_if! {
    if #[cfg(web)] {
        // This must remain conditional as otherwise it pulls in JavaScript symbols
        // on-chain (on any Wasm target).
        pub use web_time::*;
        pub use linera_kywasmtime as timer;
    } else {
        pub use std::time::*;
        pub use tokio::time as timer;

        /// A measurement of a monotonically increasing clock, from the same source that
        /// [`timer`] schedules against.
        ///
        /// This deliberately shadows `std::time::Instant` from the glob above. The two are
        /// the same clock in a normal runtime, but under `tokio::time::pause` — which is
        /// how tests drive timeouts and backoff without sleeping — only this one advances
        /// with the timers. Code that measured elapsed time with `std::time::Instant` while
        /// waiting on [`timer`] would see a deadline fire with no time having passed.
        pub use tokio::time::Instant;
    }
}

#[cfg(all(test, not(web)))]
mod tests {
    use super::*;

    /// Elapsed time and scheduled time must come from the same clock, so that code which
    /// waits on [`timer`] and then measures how long it waited agrees with itself. Under a
    /// paused clock a `std::time::Instant` would report roughly zero here.
    #[tokio::test(start_paused = true)]
    async fn test_instant_advances_with_the_timer() {
        let start = Instant::now();

        timer::sleep(Duration::from_secs(60)).await;

        assert_eq!(start.elapsed(), Duration::from_secs(60));
    }

    /// The deadline a timeout enforces and the elapsed time measured around it must also
    /// agree; `communicate_with_quorum` derives its grace period from exactly this.
    #[tokio::test(start_paused = true)]
    async fn test_timeout_deadline_matches_elapsed_time() {
        let start = Instant::now();

        let timed_out = timer::timeout(
            Duration::from_secs(5),
            timer::sleep(Duration::from_secs(60)),
        )
        .await
        .is_err();

        assert!(timed_out);
        assert_eq!(start.elapsed(), Duration::from_secs(5));
    }
}
