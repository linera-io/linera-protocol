// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A background task that monitors process RSS and dynamically reduces chain worker
//! TTLs when memory usage is high.

use std::{sync::Arc, time::Duration};

use linera_core::chain_worker::DynamicTtl;
use sysinfo::{MemoryRefreshKind, Pid, ProcessRefreshKind, RefreshKind, System};
use tracing::{debug, info, warn};

/// Configuration for the memory monitor.
pub struct MemoryMonitorConfig {
    /// The RSS threshold (in bytes) at which the monitor starts reducing TTLs.
    pub memory_limit: u64,
    /// How often to poll process RSS.
    pub poll_interval: Duration,
    /// The dynamic TTLs to adjust. All of them are scaled by the same factor.
    pub ttls: Vec<Arc<DynamicTtl>>,
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
    info!(
        memory_limit_mb = config.memory_limit / (1024 * 1024),
        "Starting memory monitor"
    );
    tokio::spawn(async move {
        let pid = Pid::from_u32(std::process::id());
        let mut system = System::new_with_specifics(
            RefreshKind::nothing()
                .with_memory(MemoryRefreshKind::nothing())
                .with_processes(ProcessRefreshKind::nothing().with_memory()),
        );
        loop {
            tokio::time::sleep(config.poll_interval).await;
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
            let ratio = rss as f64 / config.memory_limit as f64;
            // Scale factor: 1.0 when ratio <= 0.5, linearly decreasing to 0.01 at ratio >= 0.9.
            let scale = if ratio <= 0.5 {
                1.0
            } else if ratio >= 0.9 {
                0.01
            } else {
                // Linear interpolation: 1.0 at 0.5, 0.01 at 0.9
                1.0 - (ratio - 0.5) * (0.99 / 0.4)
            };
            for ttl in &config.ttls {
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
