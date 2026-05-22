// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Charm-style progress rendering for the benchmark, on stderr.
//!
//! When disabled (non-TTY or `--no-progress`), every bar is a hidden no-op, so
//! callers need no branching and the orchestrator stays testable without a TTY.

use std::{borrow::Cow, time::Instant};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

/// Factory for phase bars sharing one `MultiProgress` on stderr.
#[derive(Clone)]
pub struct Progress {
    multi: Option<MultiProgress>,
}

impl Progress {
    pub fn new(enabled: bool) -> Self {
        Self {
            multi: enabled.then(MultiProgress::new),
        }
    }

    /// Start a phase. `len = None` renders a spinner; `Some(n)` a determinate bar.
    pub fn phase(&self, name: &str, len: Option<u64>) -> Phase {
        let bar = match &self.multi {
            Some(multi) => {
                let bar = match len {
                    Some(n) => ProgressBar::new(n).with_style(bar_style()),
                    None => {
                        let b = ProgressBar::new_spinner().with_style(spinner_style());
                        b.enable_steady_tick(std::time::Duration::from_millis(100));
                        b
                    }
                };
                multi.add(bar)
            }
            None => ProgressBar::hidden(),
        };
        bar.set_prefix(name.to_string());
        Phase {
            bar,
            name: name.to_string(),
            start: Instant::now(),
        }
    }

    /// Remove all bars from the terminal (call before printing the report).
    pub fn clear(&self) {
        if let Some(multi) = &self.multi {
            if let Err(error) = multi.clear() {
                tracing::debug!(%error, "failed to clear progress bars");
            }
        }
    }
}

/// A single in-flight phase line.
pub struct Phase {
    bar: ProgressBar,
    name: String,
    start: Instant,
}

impl Phase {
    pub fn inc_length(&self, n: u64) {
        self.bar.inc_length(n);
    }

    pub fn inc(&self, n: u64) {
        self.bar.inc(n);
    }

    pub fn set_message(&self, msg: impl Into<Cow<'static, str>>) {
        self.bar.set_message(msg);
    }

    pub fn finish_ok(self) {
        self.bar.set_style(done_style());
        self.bar.finish_with_message(format!(
            "\u{2713} {} ({:.1}s)",
            self.name,
            self.start.elapsed().as_secs_f64()
        ));
    }

    pub fn finish_fail(self) {
        self.bar.set_style(done_style());
        self.bar.abandon_with_message(format!(
            "\u{2717} {} ({:.1}s)",
            self.name,
            self.start.elapsed().as_secs_f64()
        ));
    }
}

fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{spinner:.cyan} {prefix:.bold} {msg}")
        .expect("valid spinner template")
}

fn bar_style() -> ProgressStyle {
    ProgressStyle::with_template("{prefix:.bold} [{bar:30.cyan/blue}] {pos}/{len} {msg}")
        .expect("valid bar template")
        .progress_chars("=>-")
}

fn done_style() -> ProgressStyle {
    ProgressStyle::with_template("{msg}").expect("valid done template")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_progress_is_inert() {
        let p = Progress::new(false);
        let phase = p.phase("L0 test", Some(3));
        phase.set_message("hi");
        phase.inc(1);
        phase.inc_length(2);
        phase.finish_ok();
        p.clear();
    }

    #[test]
    fn disabled_spinner_phase_does_not_panic() {
        let p = Progress::new(false);
        let phase = p.phase("L0 spinner", None);
        phase.finish_fail();
    }
}
