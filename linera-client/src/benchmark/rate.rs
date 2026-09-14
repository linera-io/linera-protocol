// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Finding the highest block rate a network sustains within a latency budget.
//!
//! The search is deliberately a pure state machine: it takes one observation per control
//! interval and returns the next target rate, with no clock, network or task of its own. That
//! is what lets the climb be unit-tested against synthetic networks — a saturating one, a
//! linear one, one that is simply too slow — instead of only against a live cluster, and it is
//! why this lives here rather than in the shell script it replaces.
//!
//! The shape of the search matters for what the numbers mean. It reports the **knee**: the
//! highest rate whose tail latency stayed inside the budget. Peak throughput past that point is
//! the number everyone quotes and nobody can use, because it is bought entirely with latency.

use std::time::Duration;

/// Blocks an observation window must hold before its p99 is believed: `value_at_quantile(0.99)`
/// returns the largest sample whenever n < 100, so below this the "tail" is really a maximum and
/// one slow block decides the verdict. 200 puts two samples above the 99th percentile.
pub const MIN_P99_SAMPLES: u64 = 200;

/// How long a window may wait for [`MIN_P99_SAMPLES`] before being judged on what it has.
///
/// A slow network never reaches the floor in useful time — 200 blocks at 2 bps is 100 seconds —
/// so without a ceiling the search stalls instead of measuring. Past it the window is judged
/// anyway and its `samples` count logged, making a thin tail visible rather than silently weak.
/// At campaign rates the floor is met in well under a second, so this only binds on slow runs.
pub const MAX_WINDOW_SECS: u64 = 3;

/// How long one chain may fail every commit before the run is abandoned.
///
/// A duration, not a count: an overshoot failure is a measurement — the rate is not sustainable
/// — but at a hard overshoot commits are rejected immediately, so a chain racks up dozens of
/// consecutive failures in a few hundred milliseconds, well before the controller's next tick
/// can lower the target. Counting them aborts healthy runs. What actually separates an
/// overshoot from a chain wedged by an uncertified proposal is that the wedge never recovers,
/// so the bound has to outlast a back-off: one tick plus `confirmations` windows.
pub const MAX_COMMIT_FAILURE_SECS: u64 = 30;

/// Warm-up held before the first observation, on top of the chain-start ramp.
///
/// `bad` is a one-way ratchet: the first window judged unsustainable caps every later target,
/// and no rate above it can ever be reported. A cold cache or a half-started fleet therefore
/// pins the knee below `--bps` permanently, and — since the search now halves downward rather
/// than reporting nothing — it does so while still returning a plausible-looking number.
pub const SETTLE_SECS: u64 = 5;

/// Knobs for the code that DRIVES the search, as opposed to the search itself.
///
/// Separate from [`RateSearchConfig`] because these are consumed by the control task and the
/// workers; the state machine never sees them, which is what keeps it clock-free and testable.
#[derive(Clone, Copy, Debug)]
pub struct RateControlConfig {
    /// Blocks a window must hold before its p99 is believed.
    pub min_p99_samples: u64,
    /// How long a window may wait for `min_p99_samples` before being judged on what it has.
    pub max_window_secs: u64,
    /// Warm-up held before the first observation, on top of the chain-start ramp.
    pub settle_secs: u64,
    /// How long one chain may fail every commit before the run is abandoned.
    pub max_commit_failure_secs: u64,
}

/// What to do with the observation window as it currently stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowAction {
    /// Keep collecting until the histogram holds this many samples.
    Wait(u64),
    /// Judge it now, on whatever it holds — including nothing at all.
    Judge,
}

impl RateControlConfig {
    /// Whether a window open for `elapsed` should be judged yet.
    ///
    /// Past `max_window_secs` the answer is yes regardless of how little it holds, and an EMPTY
    /// aged-out window is the case that matters: every commit failed, so there are no latencies
    /// to summarise, and waiting for samples that will never arrive leaves the search unable to
    /// learn that the rate it set is unservable.
    pub fn window_action(&self, elapsed: Duration) -> WindowAction {
        if elapsed.as_secs() >= self.max_window_secs {
            WindowAction::Judge
        } else {
            WindowAction::Wait(self.min_p99_samples)
        }
    }
}

impl Default for RateControlConfig {
    fn default() -> Self {
        Self {
            min_p99_samples: MIN_P99_SAMPLES,
            max_window_secs: MAX_WINDOW_SECS,
            settle_secs: SETTLE_SECS,
            max_commit_failure_secs: MAX_COMMIT_FAILURE_SECS,
        }
    }
}

/// The highest sustainable rate, as offered and as actually delivered.
///
/// Both, because they differ: a level is confirmed while delivering as little as
/// `min_achieved_fraction` of what was asked for, so the offered rate overstates throughput by
/// up to 25% at the default 0.8. The offered figure is what the search brackets and bisects on;
/// the delivered figure is the measurement.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Knee {
    /// The rate that was asked for.
    pub offered_bps: usize,
    /// Blocks per second actually committed at that rate.
    pub achieved_bps: f64,
}

/// How a rate search ended.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SearchOutcome {
    /// The knee was bracketed to within the configured resolution.
    Converged(Knee),
    /// A runtime limit or cancellation ended the run first; this is the best rate confirmed
    /// so far, which is a lower bound on the knee rather than the knee itself.
    CutShort(Knee),
}

/// One control interval's worth of measurement.
#[derive(Clone, Copy, Debug)]
pub struct Observation {
    /// Blocks committed per second over the interval.
    pub achieved_bps: f64,
    /// Tail latency over the interval.
    pub p99: Duration,
}

/// What the controller wants to happen next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Keep running at this target.
    Hold(usize),
    /// The knee has been bracketed to within the configured resolution.
    Converged {
        /// The highest rate observed inside the latency budget.
        best_bps: usize,
    },
}

/// Tunables for the climb.
#[derive(Clone, Copy, Debug)]
pub struct RateSearchConfig {
    /// The tail-latency budget. The knee is the last rate that stayed under it.
    pub target_p99: Duration,
    /// Where the climb starts.
    pub start_bps: usize,
    /// Multiplier applied while the network is keeping up.
    pub growth: f64,
    /// Stop once the bracket is this close, as a fraction of the lower bound.
    pub resolution: f64,
    /// A target is only believed once this many consecutive intervals agree, so a single
    /// slow interval neither ends the search nor inflates the result.
    pub confirmations: usize,
    /// An interval delivering less than this fraction of the target counts as failing, even
    /// with good latency: the rate is not actually being offered.
    pub min_achieved_fraction: f64,
}

impl Default for RateSearchConfig {
    fn default() -> Self {
        Self {
            target_p99: Duration::from_secs(1),
            start_bps: 1,
            growth: 2.0,
            resolution: 0.1,
            confirmations: 2,
            min_achieved_fraction: 0.8,
        }
    }
}

/// Climbs toward the highest rate that holds `target_p99`.
///
/// Two phases. It first moves geometrically to bracket the knee — multiplying by `growth` while
/// the network keeps up, halving while it does not — and then bisects the resulting bracket
/// until its ends are within `resolution`. Moving geometrically first matters: an additive ramp
/// puts its coarsest resolution exactly where the curve bends. Halving downward matters because
/// `start_bps` is only a guess; a search that gave up when its opening guess was too high would
/// report no knee precisely when the operator most needs the number.
#[derive(Debug)]
pub struct RateSearch {
    config: RateSearchConfig,
    target: usize,
    /// Highest rate confirmed inside the budget.
    good: Option<usize>,
    /// Blocks per second actually delivered at `good`, which is the measurement the offered
    /// rate only approximates.
    good_achieved: f64,
    /// Lowest rate confirmed outside it.
    bad: Option<usize>,
    /// Consecutive intervals agreeing about the current target.
    agreeing: usize,
    /// What they agree on, if anything yet.
    verdict: Option<bool>,
}

impl RateSearch {
    /// Starts a search at `config.start_bps`.
    pub fn new(config: RateSearchConfig) -> Self {
        Self {
            target: config.start_bps.max(1),
            config,
            good: None,
            good_achieved: 0.0,
            bad: None,
            agreeing: 0,
            verdict: None,
        }
    }

    /// The rate the generators should currently be offering.
    pub fn target_bps(&self) -> usize {
        self.target
    }

    /// Whether `observation` is acceptable: inside the latency budget, and actually delivering
    /// close to what was asked for. A run that quietly delivers a third of its target has found
    /// a ceiling just as surely as one that blows the latency budget.
    fn is_good(&self, observation: &Observation) -> bool {
        let delivered =
            observation.achieved_bps >= self.target as f64 * self.config.min_achieved_fraction;
        observation.p99 <= self.config.target_p99 && delivered
    }

    /// Folds in one interval's measurement and returns the next move.
    pub fn observe(&mut self, observation: Observation) -> Decision {
        let good = self.is_good(&observation);

        // Require agreement across intervals before acting. These runs are bimodal -- identical
        // consecutive intervals have differed by more than an order of magnitude -- so a single
        // reading is noise, not a verdict.
        if self.verdict == Some(good) {
            self.agreeing += 1;
        } else {
            self.verdict = Some(good);
            self.agreeing = 1;
        }
        if self.agreeing < self.config.confirmations {
            return Decision::Hold(self.target);
        }
        self.agreeing = 0;
        self.verdict = None;

        if good {
            // Record what was delivered only when this level actually raises the confirmed
            // rate, so the pair stays consistent.
            if self.good.is_none_or(|g| self.target > g) {
                self.good_achieved = observation.achieved_bps;
            }
            self.good = Some(self.good.map_or(self.target, |g| g.max(self.target)));
        } else {
            self.bad = Some(self.bad.map_or(self.target, |b| b.min(self.target)));
        }

        match (self.good, self.bad) {
            // Still climbing: nothing has failed yet.
            (_, None) => {
                self.target = ((self.target as f64 * self.config.growth).round() as usize)
                    .max(self.target + 1);
                Decision::Hold(self.target)
            }
            // Nothing has passed yet, so the knee is below where we started. Halve downward to
            // bracket it: `--bps` is the operator's opening guess, and guessing high must still
            // yield a measurement rather than an immediate no-knee verdict.
            (None, Some(bad)) => {
                if bad <= 1 {
                    Decision::Converged { best_bps: 0 }
                } else {
                    self.target = bad / 2;
                    Decision::Hold(self.target)
                }
            }
            (Some(good), Some(bad)) => {
                // Adjacent integers cannot be subdivided: the midpoint would be `good` again
                // and the search would re-test a rate it has already confirmed, forever. That
                // is convergence regardless of what `resolution` asks for.
                let exhausted = bad - good <= 1;
                if exhausted || (bad - good) as f64 <= good as f64 * self.config.resolution {
                    Decision::Converged { best_bps: good }
                } else {
                    self.target = good + (bad - good) / 2;
                    Decision::Hold(self.target)
                }
            }
        }
    }

    /// The best confirmed rate so far, for a search cut short by a runtime limit.
    pub fn best_so_far(&self) -> Knee {
        Knee {
            offered_bps: self.good.unwrap_or(0),
            achieved_bps: if self.good.is_some() {
                self.good_achieved
            } else {
                0.0
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// Drives a search against a synthetic network until it converges, and returns the knee.
    /// `latency_at` is the network: it maps an offered rate to the p99 it produces.
    fn converge(config: RateSearchConfig, latency_at: impl Fn(usize) -> Duration) -> usize {
        let mut search = RateSearch::new(config);
        for _ in 0..10_000 {
            let target = search.target_bps();
            let observation = Observation {
                achieved_bps: target as f64,
                p99: latency_at(target),
            };
            if let Decision::Converged { best_bps } = search.observe(observation) {
                return best_bps;
            }
        }
        panic!("search did not converge");
    }

    /// A network that is fine up to `knee` and falls off a cliff past it. The search should
    /// land just under the knee, never above it.
    #[test]
    fn it_finds_a_hard_knee_without_overshooting() {
        for knee in [37, 100, 512, 4096] {
            let found = converge(RateSearchConfig::default(), |bps| {
                if bps <= knee {
                    ms(100)
                } else {
                    ms(5_000)
                }
            });
            assert!(
                found <= knee,
                "reported {found} as sustainable but the knee is {knee}"
            );
            assert!(
                found as f64 >= knee as f64 * 0.85,
                "reported {found}, more than 15% below the real knee {knee}"
            );
        }
    }

    /// Latency that climbs smoothly with load: the knee is where it crosses the budget.
    #[test]
    fn it_finds_a_gradual_knee() {
        // p99 = 1ms per bps, so the 1s budget is crossed at 1000 bps.
        let found = converge(RateSearchConfig::default(), |bps| ms(bps as u64));
        assert!(
            (900..=1000).contains(&found),
            "expected the knee near 1000 bps, got {found}"
        );
    }

    /// A network that cannot serve *any* rate has no knee to report. Reporting the start rate
    /// anyway would invent a result.
    #[test]
    fn a_network_that_never_meets_the_budget_reports_no_knee() {
        let found = converge(RateSearchConfig::default(), |_| ms(9_000));
        assert_eq!(found, 0);
    }

    /// A knee below the opening guess must still be measured. `--bps` is a guess, and the
    /// benchmark runbook opens well above what a cluster is likely to sustain, so a search that
    /// only climbed would report no knee exactly when the number matters.
    #[test]
    fn it_searches_downward_when_the_start_rate_is_too_high() {
        for knee in [1, 7, 140, 999] {
            let config = RateSearchConfig {
                start_bps: 4096,
                ..RateSearchConfig::default()
            };
            let found = converge(config, |bps| if bps <= knee { ms(10) } else { ms(5_000) });
            assert!(
                found <= knee,
                "reported {found} as sustainable but the knee is {knee}"
            );
            assert!(
                found as f64 >= knee as f64 * 0.5,
                "reported {found}, far below the real knee {knee}, from a start of 4096"
            );
        }
    }

    /// Latency inside budget is not enough on its own: if the generators cannot actually offer
    /// the target, the rate is not sustainable at that target either.
    #[test]
    fn falling_short_of_the_target_counts_as_failure() {
        let config = RateSearchConfig::default();
        let mut search = RateSearch::new(config);
        let target = search.target_bps();
        // Great latency, but only a third of the requested rate is being delivered.
        let observation = Observation {
            achieved_bps: target as f64 / 3.0,
            p99: ms(1),
        };
        for _ in 0..config.confirmations {
            search.observe(observation);
        }
        assert_eq!(
            search.best_so_far().offered_bps,
            0,
            "a target that was never actually offered was counted as sustained"
        );
    }

    /// One bad interval in a bimodal run must not end the climb: that is exactly the noise the
    /// confirmation count exists to absorb.
    #[test]
    fn a_single_slow_interval_does_not_end_the_search() {
        let mut search = RateSearch::new(RateSearchConfig::default());
        let start = search.target_bps();

        search.observe(Observation {
            achieved_bps: start as f64,
            p99: ms(50),
        });
        // A blip, immediately contradicted.
        search.observe(Observation {
            achieved_bps: start as f64,
            p99: ms(9_000),
        });
        search.observe(Observation {
            achieved_bps: start as f64,
            p99: ms(50),
        });
        search.observe(Observation {
            achieved_bps: start as f64,
            p99: ms(50),
        });

        assert!(
            search.target_bps() > start,
            "the climb stalled at {start} because of one slow interval"
        );
        assert_eq!(search.best_so_far().offered_bps, start);
    }

    /// A level is confirmed while delivering as little as `min_achieved_fraction` of its
    /// target, so the offered rate overstates throughput. Both must be carried, or the number
    /// we publish is up to 25% above what the network actually committed.
    #[test]
    fn the_knee_reports_what_was_delivered_not_only_what_was_asked() {
        let config = RateSearchConfig::default();
        let mut search = RateSearch::new(config);
        let target = search.target_bps();
        // Inside the latency budget and inside the 80% floor, but short of the target.
        let observation = Observation {
            achieved_bps: target as f64 * 0.85,
            p99: ms(10),
        };
        for _ in 0..config.confirmations {
            search.observe(observation);
        }
        let knee = search.best_so_far();
        assert_eq!(
            knee.offered_bps, target,
            "the confirmed level is the target"
        );
        assert!(
            (knee.achieved_bps - target as f64 * 0.85).abs() < f64::EPSILON,
            "reported {} delivered at an offered {target}, but only {} was committed",
            knee.achieved_bps,
            target as f64 * 0.85
        );
    }

    /// The floor applies only while the window is young; once it ages out the window is judged
    /// on whatever it holds. Mutating the comparison here must fail this test — the inline
    /// version of this decision was untestable, so the bug it hid survived five review rounds.
    #[test]
    fn an_aged_out_window_is_judged_however_little_it_holds() {
        let config = RateControlConfig::default();
        assert_eq!(
            config.window_action(Duration::from_secs(0)),
            WindowAction::Wait(config.min_p99_samples),
            "a young window must still wait for enough samples to support a p99"
        );
        assert_eq!(
            config.window_action(Duration::from_secs(config.max_window_secs)),
            WindowAction::Judge,
            "an aged-out window must be judged rather than waiting for samples that a dead \
             fleet will never produce"
        );
    }

    /// A window where every commit failed delivers nothing, and the search must treat that as
    /// the rate being unservable and come down. Without it the controller holds an unservable
    /// rate indefinitely, which is what a real run did: both chains failing to reach quorum for
    /// 30 seconds while the search never observed a thing.
    #[test]
    fn a_window_that_delivered_nothing_forces_the_search_down() {
        let config = RateSearchConfig {
            start_bps: 512,
            ..RateSearchConfig::default()
        };
        let mut search = RateSearch::new(config);
        let start = search.target_bps();
        let dead = Observation {
            achieved_bps: 0.0,
            p99: ms(3_000),
        };
        for _ in 0..config.confirmations {
            search.observe(dead);
        }
        assert!(
            search.target_bps() < start,
            "the search stayed at {start} after a window that committed nothing"
        );
    }

    /// Convergence is on the bracket, so a tighter resolution must not loop forever.
    #[test]
    fn a_tight_resolution_still_terminates() {
        let config = RateSearchConfig {
            resolution: 0.001,
            ..RateSearchConfig::default()
        };
        let found = converge(config, |bps| if bps <= 777 { ms(10) } else { ms(3_000) });
        assert!((770..=777).contains(&found), "got {found}");
    }
}
