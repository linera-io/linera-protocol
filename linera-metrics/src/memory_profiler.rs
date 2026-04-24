// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Safe jemalloc memory profiling with pprof output.
//!
//! This module provides HTTP endpoints for pprof format profiles using pull model.
//! Profiles are generated on-demand when endpoints are requested by Grafana Alloy.

use std::{
    ffi::CString,
    io::BufReader,
    sync::{Arc, LazyLock},
    time::Instant,
};

use axum::{
    http::{header, StatusCode},
    response::IntoResponse,
};
use linera_jemalloc_ctl::raw;
use mappings::MAPPINGS;
use pprof_util::{parse_jeheap, FlamegraphOptions, ProfStartTime};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{error, info, trace};

#[derive(Debug, Error)]
pub enum MemoryProfilerError {
    #[error("jemalloc profiling not activated - check malloc_conf configuration")]
    JemallocProfilingNotActivated,
    #[error("PROF_CTL not available - ensure jemalloc is built with profiling support")]
    ProfCtlNotAvailable,
    #[error("another profiler is already running")]
    AnotherProfilerAlreadyRunning,
    #[error("failed to activate jemalloc profiling: {0}")]
    ActivationFailed(String),
}

/// Per-process singleton for controlling jemalloc profiling.
static PROF_CTL: LazyLock<Option<Arc<Mutex<JemallocProfCtl>>>> =
    LazyLock::new(|| JemallocProfCtl::get().map(|ctl| Arc::new(Mutex::new(ctl))));

/// A handle to control jemalloc profiling.
struct JemallocProfCtl {
    start_time: Option<ProfStartTime>,
}

impl JemallocProfCtl {
    fn get() -> Option<Self> {
        // SAFETY: `opt.prof` is documented as readable and returning a bool.
        let prof_enabled: bool = unsafe { raw::read(b"opt.prof\0") }.ok()?;
        if !prof_enabled {
            return None;
        }
        // SAFETY: `opt.prof_active` is documented as readable and returning a bool.
        let prof_active: bool = unsafe { raw::read(b"opt.prof_active\0") }.ok()?;
        let start_time = prof_active.then_some(ProfStartTime::TimeImmemorial);
        Some(Self { start_time })
    }

    fn activated(&self) -> bool {
        self.start_time.is_some()
    }

    fn activate(&mut self) -> Result<(), linera_jemalloc_ctl::Error> {
        // SAFETY: `prof.active` is documented as writable and taking a bool.
        unsafe { raw::write(b"prof.active\0", true) }?;
        if self.start_time.is_none() {
            self.start_time = Some(ProfStartTime::Instant(Instant::now()));
        }
        Ok(())
    }

    fn dump(&mut self) -> anyhow::Result<std::fs::File> {
        let f = NamedTempFile::new()?;
        let path = CString::new(f.path().as_os_str().as_encoded_bytes())?;
        // SAFETY: `prof.dump` is documented as writable and taking a C string.
        unsafe { raw::write(b"prof.dump\0", path.as_ptr()) }?;
        Ok(f.into_file())
    }

    fn dump_pprof(&mut self) -> anyhow::Result<Vec<u8>> {
        let f = self.dump()?;
        let profile = parse_jeheap(BufReader::new(f), MAPPINGS.as_deref())?;
        Ok(profile.to_pprof(("inuse_space", "bytes"), ("space", "bytes"), None))
    }

    fn dump_flamegraph(&mut self) -> anyhow::Result<Vec<u8>> {
        let mut opts = FlamegraphOptions::default();
        opts.title = "inuse_space".to_string();
        opts.count_name = "bytes".to_string();
        let f = self.dump()?;
        let profile = parse_jeheap(BufReader::new(f), MAPPINGS.as_deref())?;
        profile.to_flamegraph(&mut opts)
    }
}

/// Memory profiler using linera-jemalloc-ctl directly (pull model only).
pub struct MemoryProfiler;

impl MemoryProfiler {
    /// Activates jemalloc profiling at runtime.
    ///
    /// Sampling (prof_active) is off by default to avoid the libgcc DWARF
    /// unwinder livelock (jemalloc/jemalloc#2282).
    pub async fn activate() -> Result<(), MemoryProfilerError> {
        if let Some(prof_ctl) = PROF_CTL.as_ref() {
            let mut prof_ctl = prof_ctl.lock().await;

            prof_ctl
                .activate()
                .map_err(|e| MemoryProfilerError::ActivationFailed(e.to_string()))?;

            info!("jemalloc memory profiling activated");
            Ok(())
        } else {
            Err(MemoryProfilerError::ProfCtlNotAvailable)
        }
    }

    pub fn check_prof_ctl() -> Result<(), MemoryProfilerError> {
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

    async fn collect_heap_profile() -> anyhow::Result<Vec<u8>> {
        let prof_ctl = PROF_CTL
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PROF_CTL not available"))?;
        let mut prof_ctl = prof_ctl.lock().await;

        if !prof_ctl.activated() {
            return Err(anyhow::anyhow!("jemalloc profiling not activated"));
        }

        let profile = prof_ctl.dump_pprof()?;
        trace!("✓ Collected heap profile ({} bytes)", profile.len());
        Ok(profile)
    }

    async fn collect_heap_flamegraph() -> anyhow::Result<Vec<u8>> {
        let prof_ctl = PROF_CTL
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PROF_CTL not available"))?;
        let mut prof_ctl = prof_ctl.lock().await;

        if !prof_ctl.activated() {
            return Err(anyhow::anyhow!("jemalloc profiling not activated"));
        }

        let flamegraph = prof_ctl.dump_flamegraph()?;
        trace!("✓ Generated flamegraph ({} bytes)", flamegraph.len());
        Ok(flamegraph)
    }
}
