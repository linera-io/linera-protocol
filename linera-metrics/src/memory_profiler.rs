// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Safe jemalloc memory profiling with jemalloc_pprof integration
//!
//! This module provides HTTP endpoints for pprof format profiles using pull model.
//! Profiles are generated on-demand when endpoints are requested by Grafana Alloy.

use axum::{
    http::{header, StatusCode},
    response::IntoResponse,
};
use jemalloc_pprof::PROF_CTL;
use thiserror::Error;
use tracing::{error, info, trace};

#[derive(Debug, Error)]
pub enum MemoryProfilerError {
    #[error("jemalloc profiling not activated - check malloc_conf configuration")]
    JemallocProfilingNotActivated,
    #[error("PROF_CTL not available - ensure jemalloc is built with profiling support")]
    ProfCtlNotAvailable,
    #[error("another profiler is already running")]
    AnotherProfilerAlreadyRunning,
    #[error("failed to activate profiling: {0}")]
    ActivationFailed(String),
}

/// Memory profiler using safe jemalloc_pprof wrapper (pull model only)
pub struct MemoryProfiler;

impl MemoryProfiler {
    /// Activate memory profiling at runtime.
    /// Call this early in main() when --enable-memory-profiling is passed.
    /// This sets prof.active=true via mallctl.
    pub async fn activate() -> Result<(), MemoryProfilerError> {
        if let Some(prof_ctl) = PROF_CTL.as_ref() {
            let mut prof_ctl = prof_ctl.lock().await;

            prof_ctl
                .activate()
                .map_err(|e| MemoryProfilerError::ActivationFailed(e.to_string()))?;

            info!("Memory profiling activated");
            Ok(())
        } else {
            error!("PROF_CTL not available");
            Err(MemoryProfilerError::ProfCtlNotAvailable)
        }
    }

    pub fn check_prof_ctl() -> Result<(), MemoryProfilerError> {
        // Check if jemalloc profiling is available
        if let Some(prof_ctl) = PROF_CTL.as_ref() {
            let prof_ctl = prof_ctl
                .try_lock()
                .map_err(|_| MemoryProfilerError::AnotherProfilerAlreadyRunning)?;

            if !prof_ctl.activated() {
                error!("jemalloc profiling not activated");
                return Err(MemoryProfilerError::JemallocProfilingNotActivated);
            }

            trace!("✓ jemalloc memory profiling is ready");
        } else {
            error!("PROF_CTL not available");
            return Err(MemoryProfilerError::ProfCtlNotAvailable);
        }

        Ok(())
    }

    /// HTTP endpoint for heap profile - returns fresh pprof data
    pub async fn heap_profile() -> Result<impl IntoResponse, StatusCode> {
        trace!("Serving heap profile via /debug/pprof");

        match Self::collect_heap_profile().await {
            Ok(profile_data) => {
                trace!("✓ Serving heap profile ({} bytes)", profile_data.len());
                Ok((
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/octet-stream")],
                    profile_data,
                ))
            }
            Err(e) => {
                error!("Failed to collect heap profile: {}", e);
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }

    /// HTTP endpoint for flamegraph - returns SVG flamegraph
    pub async fn heap_flamegraph() -> Result<impl IntoResponse, StatusCode> {
        info!("Serving heap flamegraph via /debug/flamegraph");

        match Self::collect_heap_flamegraph().await {
            Ok(flamegraph_svg) => {
                trace!("✓ Serving heap flamegraph ({} bytes)", flamegraph_svg.len());
                Ok((
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "image/svg+xml")],
                    flamegraph_svg,
                ))
            }
            Err(e) => {
                error!("Failed to collect heap flamegraph: {}", e);
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }

    /// Collect heap profile on-demand using safe jemalloc_pprof wrapper
    async fn collect_heap_profile() -> anyhow::Result<Vec<u8>> {
        if let Some(prof_ctl) = PROF_CTL.as_ref() {
            let mut prof_ctl = prof_ctl.lock().await;

            if !prof_ctl.activated() {
                return Err(anyhow::anyhow!("jemalloc profiling not activated"));
            }

            match prof_ctl.dump_pprof() {
                Ok(profile) => {
                    trace!("✓ Collected heap profile ({} bytes)", profile.len());
                    Ok(profile)
                }
                Err(e) => {
                    error!("Failed to dump pprof profile: {}", e);
                    Err(anyhow::anyhow!("Failed to dump pprof profile: {}", e))
                }
            }
        } else {
            Err(anyhow::anyhow!("PROF_CTL not available"))
        }
    }

    /// Collect heap flamegraph using prof_ctl.dump_flamegraph()
    async fn collect_heap_flamegraph() -> anyhow::Result<Vec<u8>> {
        if let Some(prof_ctl) = PROF_CTL.as_ref() {
            let mut prof_ctl = prof_ctl.lock().await;

            if !prof_ctl.activated() {
                return Err(anyhow::anyhow!("jemalloc profiling not activated"));
            }

            match prof_ctl.dump_flamegraph() {
                Ok(flamegraph_bytes) => {
                    trace!("✓ Generated flamegraph ({} bytes)", flamegraph_bytes.len());
                    Ok(flamegraph_bytes)
                }
                Err(e) => {
                    error!("Failed to dump flamegraph: {}", e);
                    Err(anyhow::anyhow!("Failed to dump flamegraph: {}", e))
                }
            }
        } else {
            Err(anyhow::anyhow!("PROF_CTL not available"))
        }
    }
}
