// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A background task that monitors process RSS and dynamically reduces chain worker
//! TTLs when memory usage is high.

use std::{sync::Arc, time::Duration};

use linera_core::chain_worker::DynamicTtl;
use sysinfo::{MemoryRefreshKind, Pid, ProcessRefreshKind, RefreshKind, System};
use tracing::{debug, info, warn};

/// Default polling interval for the memory monitor.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Default memory limit: 60% of total system (or cgroup) memory.
pub const DEFAULT_MEMORY_LIMIT: &str = "60%";

/// A memory limit specified either as an absolute number of megabytes or as a
/// percentage of total system (or cgroup) memory.
#[derive(Debug, Clone)]
pub enum MemoryLimit {
    /// Absolute limit in megabytes.
    Megabytes(u64),
    /// Fraction of total system memory (0.0–1.0).
    Fraction(f64),
}

impl MemoryLimit {
    /// Resolves the limit to an absolute byte count.
    fn resolve(&self) -> u64 {
        match *self {
            MemoryLimit::Megabytes(mb) => mb * 1024 * 1024,
            MemoryLimit::Fraction(f) => {
                let total = detect_total_memory();
                let limit = (total as f64 * f) as u64;
                info!(
                    total_mb = total / (1024 * 1024),
                    limit_mb = limit / (1024 * 1024),
                    "Using {:.0}% of total memory as chain worker memory limit",
                    f * 100.0,
                );
                limit
            }
        }
    }
}

/// Parses a memory limit string: either a plain number (megabytes) or a number
/// followed by `%` (percentage of total memory). E.g. `"4096"` or `"60%"`.
pub fn parse_memory_limit(s: &str) -> Result<MemoryLimit, String> {
    if let Some(pct) = s.strip_suffix('%') {
        let value: f64 = pct
            .trim()
            .parse()
            .map_err(|e| format!("invalid percentage: {e}"))?;
        if !(0.0..=100.0).contains(&value) {
            return Err(format!("percentage must be between 0 and 100, got {value}"));
        }
        Ok(MemoryLimit::Fraction(value / 100.0))
    } else {
        let mb: u64 = s
            .trim()
            .parse()
            .map_err(|e| format!("invalid megabyte value: {e}"))?;
        Ok(MemoryLimit::Megabytes(mb))
    }
}

/// Configuration for the memory monitor.
pub struct MemoryMonitorConfig {
    /// The RSS threshold at which the monitor starts reducing TTLs.
    pub memory_limit: MemoryLimit,
    /// How often to poll process RSS.
    pub poll_interval: Duration,
    /// The dynamic TTLs to adjust. All of them are scaled by the same factor.
    pub ttls: Vec<Arc<DynamicTtl>>,
}

/// Returns total system memory in bytes. On Linux inside a cgroup (container),
/// `sysinfo` reports the cgroup memory limit rather than host RAM.
fn detect_total_memory() -> u64 {
    let system = System::new_with_specifics(
        RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
    );
    system.total_memory()
}

/// Spawns a background task that monitors memory usage and adjusts chain worker TTLs.
///
/// The TTL is scaled down in proportion to how close the process is to the memory
/// limit. Above 90% of the limit, the TTL is reduced to 1% of its base value to
/// aggressively evict idle workers.
pub fn spawn_memory_monitor(config: MemoryMonitorConfig) {
    if config.ttls.is_empty() {
        return;
    }
    let memory_limit = config.memory_limit.resolve();
    info!(
        memory_limit_mb = memory_limit / (1024 * 1024),
        poll_interval_ms = config.poll_interval.as_millis() as u64,
        "Starting memory monitor"
    );
    let poll_interval = config.poll_interval;
    let ttls = config.ttls;
    tokio::spawn(async move {
        let pid = Pid::from_u32(std::process::id());
        let mut system = System::new_with_specifics(
            RefreshKind::nothing()
                .with_memory(MemoryRefreshKind::nothing())
                .with_processes(ProcessRefreshKind::nothing().with_memory()),
        );
        if memory_limit == 0 {
            tracing::error!("disabling memory monitor: memory limit is 0");
            return;
        }
        loop {
            tokio::time::sleep(poll_interval).await;
            system.refresh_processes_specifics(
                sysinfo::ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::nothing().with_memory(),
            );
            let Some(process) = system.process(pid) else {
                warn!("Memory monitor: could not find own process");
                continue;
            };
            let rss = process.memory();
            let ratio = rss as f64 / memory_limit as f64;
            // Scale factor: 1.0 when ratio <= 0.5, linearly decreasing to 0.01 at ratio >= 0.9.
            let scale = if ratio <= 0.5 {
                1.0
            } else if ratio >= 0.9 {
                0.01
            } else {
                // Linear interpolation: 1.0 at 0.5, 0.01 at 0.9
                1.0 - (ratio - 0.5) * (0.99 / 0.4)
            };
            for ttl in &ttls {
                let base = ttl.base();
                let new = Duration::from_secs_f64(base.as_secs_f64() * scale)
                    .max(Duration::from_millis(10));
                ttl.set(new);
            }
            debug!(
                rss_mb = rss / (1024 * 1024),
                ratio = format!("{:.1}%", ratio * 100.0),
                scale = format!("{:.3}", scale),
                "Memory monitor tick"
            );
        }
    });
}
