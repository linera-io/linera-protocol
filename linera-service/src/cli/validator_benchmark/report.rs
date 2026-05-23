// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Benchmark report data model, output-spec parsing, and serializers.

use std::path::PathBuf;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

#[cfg(test)]
use super::latency::ErrorBucket;
use super::latency::LatencySummary;

/// Output format requested on the command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Json,
    Yaml,
    Md,
    Brief,
}

/// Where a rendered format is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Stdout,
    File(PathBuf),
}

/// One requested `<format>[:<path>]` output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputSpec {
    pub format: Format,
    pub target: Target,
}

impl OutputSpec {
    /// Parse all `--output` values. Each value may carry several specs separated
    /// by `,` or `+`. An empty input defaults to Markdown on stdout.
    pub fn parse_all(raw: &[String]) -> Result<Vec<OutputSpec>> {
        if raw.is_empty() {
            return Ok(vec![OutputSpec {
                format: Format::Md,
                target: Target::Stdout,
            }]);
        }
        let mut out = Vec::new();
        for item in raw {
            for part in item.split([',', '+']) {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                out.push(Self::parse_one(part)?);
            }
        }
        if out.is_empty() {
            return Err(anyhow!("--output specified but resolved to no entries"));
        }
        Ok(out)
    }

    fn parse_one(s: &str) -> Result<OutputSpec> {
        let (fmt_str, target) = match s.split_once(':') {
            Some((f, p)) => (f, Target::File(PathBuf::from(p))),
            None => (s, Target::Stdout),
        };
        let format = match fmt_str {
            "json" => Format::Json,
            "yaml" => Format::Yaml,
            "md" => Format::Md,
            "brief" => Format::Brief,
            other => {
                return Err(anyhow!(
                    "unknown output format `{other}` (expected json|yaml|md|brief)"
                ))
            }
        };
        Ok(OutputSpec { format, target })
    }
}

/// Top-level benchmark report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    pub metadata: Metadata,
    pub layers: Layers,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub tool_version: String,
    pub candidate: Candidate,
    pub observer: Observer,
    pub config: serde_json::Value,
    pub chains_tested: Vec<String>,
    /// `false` while the run is in progress (a file flushed after each layer),
    /// `true` only on the final write. Lets a reader tell a partial run from a
    /// complete one.
    pub complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candidate {
    pub address: String,
    pub public_key: Option<String>,
    /// Debug rendering of the validator's `VersionInfo` (the type is not
    /// `Serialize` outside the version-build context, so it is captured as text).
    pub version_info: Option<String>,
    pub network_description: Option<serde_json::Value>,
}

/// Where and when the benchmark ran. Timestamps are RFC 3339 strings to avoid
/// pulling chrono's `serde` feature into this crate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observer {
    pub location: String,
    pub hostname: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub duration_secs: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Layers {
    pub preflight: Option<PreflightReport>,
    pub read_baseline: Option<ReadBaselineReport>,
    pub read_stress: Option<ReadStressReport>,
    pub bulk_download: Option<BulkDownloadReport>,
    pub tip_lag: Option<TipLagReport>,
    pub partial_sync: Option<PartialSyncReport>,
}

// Per-layer report shapes are introduced as concrete structs when each layer is
// implemented. Until then they are JSON-value placeholders so the model and the
// serializers can be built and tested independently.

/// L1 preflight: reachability, version, network description, baseline RTT.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PreflightReport {
    pub status: PreflightStatus,
    pub rtt_ms: LatencySummary,
    pub version_match: Option<bool>,
    pub network_description_match: Option<bool>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PreflightStatus {
    #[default]
    Ok,
    Fail,
}

/// L2 read latency baseline: sequential `handle_chain_info_query` per chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadBaselineReport {
    pub per_chain: Vec<PerChainReadBaseline>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerChainReadBaseline {
    pub chain_id: String,
    pub latency_ms: LatencySummary,
}

/// L3 read stress: a concurrency ramp over `handle_chain_info_query` per chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadStressReport {
    pub per_chain: Vec<PerChainReadStress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerChainReadStress {
    pub chain_id: String,
    pub levels: Vec<StressLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StressLevel {
    pub concurrency: usize,
    pub duration_secs: u64,
    pub completed: u64,
    pub throughput_per_sec: f64,
    pub latency_ms: LatencySummary,
}

/// L4 bulk download: `download_certificates_by_heights` over a height range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkDownloadReport {
    pub per_chain: Vec<PerChainBulk>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerChainBulk {
    pub chain_id: String,
    pub runs: Vec<BulkRun>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkRun {
    pub concurrency: usize,
    pub batch_size: u32,
    pub heights_range: (u64, u64),
    pub bytes_in: u64,
    pub certs_received: u64,
    pub duration_secs: f64,
    pub mb_per_sec: f64,
    pub certs_per_sec: f64,
    pub latency_ms: LatencySummary,
}

/// L5 tip-lag snapshot: candidate tip vs the committee-backed reference tip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TipLagReport {
    pub per_chain: Vec<PerChainTipLag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerChainTipLag {
    pub chain_id: String,
    pub samples: Vec<TipLagSample>,
    pub trend: TipLagTrend,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TipLagSample {
    pub t_secs: u64,
    pub candidate_tip: u64,
    pub reference_tip: u64,
    /// `reference_tip - candidate_tip`; positive means the candidate is behind.
    pub lag_blocks: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TipLagTrend {
    Converging,
    Stable,
    Diverging,
}

/// L6 partial sync: write-path ingest of a bounded run of blocks (opt-in).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialSyncReport {
    pub chain_id: String,
    pub from_height: u64,
    pub to_height: u64,
    pub blocks_attempted: u64,
    pub blocks_accepted: u64,
    pub bytes_in: u64,
    pub duration_secs: f64,
    pub blocks_per_sec: f64,
}

/// First 8 hex chars of a chain id with an ellipsis, for compact tables.
fn short(chain_id: &str) -> String {
    let head: String = chain_id.chars().take(8).collect();
    format!("{head}…")
}

/// `min/p50/p95/p99/max` rendered as integer milliseconds.
fn latency_line(s: &LatencySummary) -> String {
    format!(
        "min {:.0} / p50 {:.0} / p95 {:.0} / p99 {:.0} / max {:.0}",
        s.min, s.p50, s.p95, s.p99, s.max
    )
}

fn errors_total(s: &LatencySummary) -> u64 {
    s.errors.iter().map(|e| e.count).sum()
}

fn trend_str(trend: TipLagTrend) -> &'static str {
    match trend {
        TipLagTrend::Converging => "converging",
        TipLagTrend::Stable => "stable",
        TipLagTrend::Diverging => "diverging",
    }
}

/// Headline metrics distilled from the full report, shared by the brief recap and
/// the Markdown summary table so the two never drift.
struct Highlights {
    preflight_ok: Option<bool>,
    rtt_p50: Option<f64>,
    rtt_p99: Option<f64>,
    read: Option<(f64, f64, f64)>,
    peak_throughput: Option<(f64, usize)>,
    bulk: Option<(f64, f64)>,
    tip_lag: Option<(i64, TipLagTrend)>,
    partial_bps: Option<f64>,
    errors: std::collections::BTreeMap<String, u64>,
}

fn highlights(r: &Report) -> Highlights {
    let l = &r.layers;
    let mut errors: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let mut tally = |s: &LatencySummary| {
        for e in &s.errors {
            *errors.entry(e.category.clone()).or_default() += e.count;
        }
    };

    let (preflight_ok, rtt_p50, rtt_p99) = match &l.preflight {
        Some(p) => {
            tally(&p.rtt_ms);
            (
                Some(matches!(p.status, PreflightStatus::Ok)),
                Some(p.rtt_ms.p50),
                Some(p.rtt_ms.p99),
            )
        }
        None => (None, None, None),
    };

    let read = l.read_baseline.as_ref().map(|b| {
        let mut p50 = 0.0_f64;
        let mut p95 = 0.0_f64;
        let mut p99 = 0.0_f64;
        for c in &b.per_chain {
            tally(&c.latency_ms);
            p50 = p50.max(c.latency_ms.p50);
            p95 = p95.max(c.latency_ms.p95);
            p99 = p99.max(c.latency_ms.p99);
        }
        (p50, p95, p99)
    });

    let peak_throughput = l.read_stress.as_ref().and_then(|s| {
        let mut best: Option<(f64, usize)> = None;
        for c in &s.per_chain {
            for level in &c.levels {
                tally(&level.latency_ms);
                if best.is_none_or(|(t, _)| level.throughput_per_sec > t) {
                    best = Some((level.throughput_per_sec, level.concurrency));
                }
            }
        }
        best
    });

    let bulk = l.bulk_download.as_ref().and_then(|b| {
        let mut mbps = 0.0_f64;
        let mut certs = 0.0_f64;
        let mut any = false;
        for c in &b.per_chain {
            for run in &c.runs {
                tally(&run.latency_ms);
                mbps = mbps.max(run.mb_per_sec);
                certs = certs.max(run.certs_per_sec);
                any = true;
            }
        }
        any.then_some((mbps, certs))
    });

    let tip_lag = l.tip_lag.as_ref().and_then(|t| {
        let mut worst: Option<(i64, TipLagTrend)> = None;
        for c in &t.per_chain {
            if let Some(last) = c.samples.last() {
                if worst.is_none_or(|(lag, _)| last.lag_blocks > lag) {
                    worst = Some((last.lag_blocks, c.trend));
                }
            }
        }
        worst
    });

    let partial_bps = l.partial_sync.as_ref().map(|p| p.blocks_per_sec);

    Highlights {
        preflight_ok,
        rtt_p50,
        rtt_p99,
        read,
        peak_throughput,
        bulk,
        tip_lag,
        partial_bps,
        errors,
    }
}

fn errors_inline(errors: &std::collections::BTreeMap<String, u64>) -> String {
    if errors.is_empty() {
        return "0".to_string();
    }
    errors
        .iter()
        .map(|(cat, n)| format!("{n} {cat}"))
        .collect::<Vec<_>>()
        .join(" · ")
}

/// Render a compact recap: header line plus a short list of headline results.
pub fn render_brief(r: &Report) -> String {
    let m = &r.metadata;
    let h = highlights(r);
    let mut s = String::new();
    s.push_str(&format!("Validator Benchmark — {}\n", m.candidate.address));
    s.push_str(&format!(
        "Observer: {} · {} · {} · {}\n",
        m.observer.location,
        m.observer.started_at,
        m.observer
            .duration_secs
            .map_or_else(|| "running".to_string(), |d| format!("{d}s")),
        if m.complete { "COMPLETE" } else { "PARTIAL" },
    ));
    if let Some(ok) = h.preflight_ok {
        let rtt = match (h.rtt_p50, h.rtt_p99) {
            (Some(p50), Some(p99)) => format!("rtt p50 {p50:.0}ms / p99 {p99:.0}ms"),
            _ => String::new(),
        };
        s.push_str(&format!(
            "  Preflight        {}            {rtt}\n",
            if ok { "✓ OK" } else { "✗ FAIL" }
        ));
    }
    if let Some((p50, p95, p99)) = h.read {
        s.push_str(&format!(
            "  Read latency     p50 {p50:.0}ms · p95 {p95:.0}ms · p99 {p99:.0}ms\n"
        ));
    }
    if let Some((tput, conc)) = h.peak_throughput {
        s.push_str(&format!(
            "  Read throughput  peak {tput:.0} req/s @ conc {conc}\n"
        ));
    }
    if let Some((mbps, certs)) = h.bulk {
        s.push_str(&format!(
            "  Bulk download    {mbps:.1} MB/s · {certs:.0} certs/s\n"
        ));
    }
    if let Some((lag, trend)) = h.tip_lag {
        s.push_str(&format!(
            "  Tip lag          {lag} blocks behind · {}\n",
            trend_str(trend)
        ));
    }
    if let Some(bps) = h.partial_bps {
        s.push_str(&format!("  Partial sync     {bps:.1} blocks/s\n"));
    }
    s.push_str(&format!(
        "  Errors           {}\n",
        errors_inline(&h.errors)
    ));
    s
}

/// Render the full human-readable Markdown report: header, recap summary, and a
/// detailed table per layer.
pub fn render_markdown(r: &Report) -> String {
    let m = &r.metadata;
    let h = highlights(r);
    let mut s = String::new();

    s.push_str("# Validator Benchmark Report\n\n");
    s.push_str(&format!("- **Candidate:** `{}`\n", m.candidate.address));
    if let Some(pk) = &m.candidate.public_key {
        s.push_str(&format!("- **Public key:** `{pk}`\n"));
    }
    if let Some(v) = &m.candidate.version_info {
        s.push_str(&format!("- **Version:** {v}\n"));
    }
    s.push_str(&format!(
        "- **Observer:** {} @ {} · Started {} · Duration {} · Complete: {}\n",
        m.observer.location,
        m.observer.hostname,
        m.observer.started_at,
        m.observer
            .duration_secs
            .map_or_else(|| "—".to_string(), |d| format!("{d}s")),
        if m.complete { "yes" } else { "no (partial)" },
    ));
    s.push_str(&format!("- **Chains:** {}\n", m.chains_tested.join(", ")));
    if let Some(t) = m.config.get("rpc_timeout_secs").and_then(|v| v.as_u64()) {
        s.push_str(&format!("- **RPC timeout:** {t}s\n"));
    }
    s.push('\n');

    // Recap.
    s.push_str("## Summary\n\n| Metric | Value |\n|---|---|\n");
    if let Some(ok) = h.preflight_ok {
        let rtt = match (h.rtt_p50, h.rtt_p99) {
            (Some(p50), Some(p99)) => format!(" (rtt p50 {p50:.0}ms, p99 {p99:.0}ms)"),
            _ => String::new(),
        };
        s.push_str(&format!(
            "| Preflight | {}{rtt} |\n",
            if ok { "✓ OK" } else { "✗ FAIL" }
        ));
    }
    if let Some((p50, p95, p99)) = h.read {
        s.push_str(&format!(
            "| Read latency (p50/p95/p99) | {p50:.0} / {p95:.0} / {p99:.0} ms |\n"
        ));
    }
    if let Some((tput, conc)) = h.peak_throughput {
        s.push_str(&format!(
            "| Peak sustained throughput | {tput:.0} req/s @ conc {conc} |\n"
        ));
    }
    if let Some((mbps, certs)) = h.bulk {
        s.push_str(&format!(
            "| Bulk download | {mbps:.1} MB/s · {certs:.0} certs/s |\n"
        ));
    }
    match h.tip_lag {
        Some((lag, trend)) => s.push_str(&format!(
            "| Tip lag (last) | {lag} blocks · {} |\n",
            trend_str(trend)
        )),
        None => s.push_str("| Tip lag (last) | — |\n"),
    }
    match h.partial_bps {
        Some(bps) => s.push_str(&format!("| Partial sync | {bps:.1} blocks/s |\n")),
        None => s.push_str("| Partial sync | — (not run) |\n"),
    }
    s.push_str(&format!(
        "| Total errors | {} |\n\n",
        errors_inline(&h.errors)
    ));

    // Detail.
    if let Some(p) = &r.layers.preflight {
        s.push_str(&format!(
            "## L1 Preflight\n\nStatus: {} · RTT ms: {}\n",
            if matches!(p.status, PreflightStatus::Ok) {
                "✓ OK"
            } else {
                "✗ FAIL"
            },
            latency_line(&p.rtt_ms),
        ));
        if !p.errors.is_empty() {
            s.push_str(&format!("\nErrors: {}\n", p.errors.join("; ")));
        }
        s.push('\n');
    }
    if let Some(b) = &r.layers.read_baseline {
        s.push_str("## L2 Read baseline\n\n| chain | count | min | p50 | p95 | p99 | max | errors |\n|---|---|---|---|---|---|---|---|\n");
        for c in &b.per_chain {
            let m = &c.latency_ms;
            s.push_str(&format!(
                "| {} | {} | {:.0} | {:.0} | {:.0} | {:.0} | {:.0} | {} |\n",
                short(&c.chain_id),
                m.count,
                m.min,
                m.p50,
                m.p95,
                m.p99,
                m.max,
                errors_total(m),
            ));
        }
        s.push('\n');
    }
    if let Some(st) = &r.layers.read_stress {
        s.push_str("## L3 Read stress\n\n| chain | conc | req/s | p50 | p95 | p99 | errors |\n|---|---|---|---|---|---|---|\n");
        for c in &st.per_chain {
            for level in &c.levels {
                let m = &level.latency_ms;
                s.push_str(&format!(
                    "| {} | {} | {:.0} | {:.0} | {:.0} | {:.0} | {} |\n",
                    short(&c.chain_id),
                    level.concurrency,
                    level.throughput_per_sec,
                    m.p50,
                    m.p95,
                    m.p99,
                    errors_total(m),
                ));
            }
        }
        s.push('\n');
    }
    if let Some(b) = &r.layers.bulk_download {
        s.push_str("## L4 Bulk download\n\n| chain | conc | MB/s | certs/s | p95 ms | errors |\n|---|---|---|---|---|---|\n");
        for c in &b.per_chain {
            for run in &c.runs {
                s.push_str(&format!(
                    "| {} | {} | {:.1} | {:.0} | {:.0} | {} |\n",
                    short(&c.chain_id),
                    run.concurrency,
                    run.mb_per_sec,
                    run.certs_per_sec,
                    run.latency_ms.p95,
                    errors_total(&run.latency_ms),
                ));
            }
        }
        s.push('\n');
    }
    if let Some(t) = &r.layers.tip_lag {
        for c in &t.per_chain {
            s.push_str(&format!(
                "## L5 Tip-lag {} (trend: {})\n\n| t(s) | candidate | reference | lag |\n|---|---|---|---|\n",
                short(&c.chain_id),
                trend_str(c.trend),
            ));
            for sample in &c.samples {
                s.push_str(&format!(
                    "| {} | {} | {} | {} |\n",
                    sample.t_secs, sample.candidate_tip, sample.reference_tip, sample.lag_blocks,
                ));
            }
            s.push('\n');
        }
    }
    if let Some(p) = &r.layers.partial_sync {
        s.push_str(&format!(
            "## L6 Partial sync\n\nChain {} · heights {}–{} · accepted {}/{} blocks · {:.1} blocks/s · {} bytes in {:.1}s\n\n",
            short(&p.chain_id),
            p.from_height,
            p.to_height,
            p.blocks_accepted,
            p.blocks_attempted,
            p.blocks_per_sec,
            p.bytes_in,
            p.duration_secs,
        ));
    }

    s
}

/// Renders a report to one or more targets (files and/or stdout).
pub struct Writer {
    targets: Vec<OutputSpec>,
}

impl Writer {
    pub fn new(targets: Vec<OutputSpec>) -> Self {
        Self { targets }
    }

    fn render(format: Format, report: &Report) -> Result<String> {
        Ok(match format {
            Format::Json => serde_json::to_string_pretty(report)?,
            Format::Yaml => serde_yaml::to_string(report)?,
            Format::Md => render_markdown(report),
            Format::Brief => render_brief(report),
        })
    }

    /// Write only the file targets, leaving stdout untouched. Called after each
    /// layer so an interrupted run still leaves the completed layers on disk.
    pub fn write_files(&self, report: &Report) -> Result<()> {
        for spec in &self.targets {
            if let Target::File(path) = &spec.target {
                std::fs::write(path, Self::render(spec.format, report)?)?;
            }
        }
        Ok(())
    }

    /// Emit the report to every configured target. When several formats share
    /// stdout, they are concatenated under `===== <FORMAT> =====` headers.
    pub fn emit(&self, report: &Report) -> Result<()> {
        use std::io::Write as _;

        let mut stdout_chunks: Vec<(Format, String)> = Vec::new();
        for spec in &self.targets {
            let rendered = Self::render(spec.format, report)?;
            match &spec.target {
                Target::Stdout => stdout_chunks.push((spec.format, rendered)),
                Target::File(path) => std::fs::write(path, rendered)?,
            }
        }
        if !stdout_chunks.is_empty() {
            let stdout = std::io::stdout();
            let mut lock = stdout.lock();
            let multi = stdout_chunks.len() > 1;
            for (format, content) in &stdout_chunks {
                if multi {
                    writeln!(lock, "===== {format:?} =====")?;
                }
                writeln!(lock, "{content}")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
fn fixture() -> Report {
    Report {
        metadata: Metadata {
            tool_version: "linera vX (test)".into(),
            candidate: Candidate {
                address: "grpcs://example:443".into(),
                public_key: None,
                version_info: None,
                network_description: None,
            },
            observer: Observer {
                location: "test".into(),
                hostname: "h".into(),
                started_at: "2026-05-21T12:34:56Z".into(),
                ended_at: None,
                duration_secs: None,
            },
            config: serde_json::Value::Null,
            chains_tested: vec!["c1".into()],
            complete: false,
        },
        layers: Layers::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_format_stdout() {
        let specs = OutputSpec::parse_all(&["md".to_string()]).unwrap();
        assert_eq!(
            specs,
            vec![OutputSpec {
                format: Format::Md,
                target: Target::Stdout
            }]
        );
    }

    #[test]
    fn parse_format_with_path() {
        let specs = OutputSpec::parse_all(&["json:/tmp/out.json".to_string()]).unwrap();
        assert_eq!(
            specs,
            vec![OutputSpec {
                format: Format::Json,
                target: Target::File("/tmp/out.json".into())
            }]
        );
    }

    #[test]
    fn parse_comma_separated() {
        let specs = OutputSpec::parse_all(&["md,json:/tmp/x.json,brief".to_string()]).unwrap();
        assert_eq!(specs.len(), 3);
        assert_eq!(specs[0].format, Format::Md);
        assert_eq!(specs[1].format, Format::Json);
        assert_eq!(specs[2].format, Format::Brief);
    }

    #[test]
    fn parse_plus_separated() {
        let specs = OutputSpec::parse_all(&["md+brief".to_string()]).unwrap();
        assert_eq!(specs.len(), 2);
    }

    #[test]
    fn parse_repeated_flag() {
        let specs = OutputSpec::parse_all(&["json".to_string(), "md".to_string()]).unwrap();
        assert_eq!(specs.len(), 2);
    }

    #[test]
    fn parse_default_when_empty() {
        let specs = OutputSpec::parse_all(&[]).unwrap();
        assert_eq!(
            specs,
            vec![OutputSpec {
                format: Format::Md,
                target: Target::Stdout
            }]
        );
    }

    #[test]
    fn parse_unknown_format_rejected() {
        assert!(OutputSpec::parse_all(&["html".to_string()]).is_err());
    }

    #[test]
    fn json_round_trip() {
        let r = fixture();
        let s = serde_json::to_string(&r).unwrap();
        let back: Report = serde_json::from_str(&s).unwrap();
        assert_eq!(
            back.metadata.candidate.address,
            r.metadata.candidate.address
        );
    }

    #[test]
    fn yaml_serializes() {
        let r = fixture();
        let s = serde_yaml::to_string(&r).unwrap();
        assert!(s.contains("grpcs://example:443"));
    }

    #[test]
    fn markdown_contains_header_and_candidate() {
        let r = fixture();
        let md = render_markdown(&r);
        assert!(md.contains("# Validator Benchmark Report"));
        assert!(md.contains("grpcs://example:443"));
    }

    #[test]
    fn brief_is_short_and_identifies_candidate() {
        let r = fixture();
        let b = render_brief(&r);
        assert!(b.lines().count() <= 6);
        assert!(b.contains("grpcs://example:443"));
    }

    fn summary(p50: f64, p95: f64, p99: f64, errors: Vec<ErrorBucket>) -> LatencySummary {
        LatencySummary {
            count: 200,
            min: 5.0,
            max: 120.0,
            mean: 12.0,
            stddev: 8.0,
            p50,
            p95,
            p99,
            errors,
        }
    }

    fn populated() -> Report {
        let mut r = fixture();
        r.metadata.complete = true;
        r.metadata.observer.duration_secs = Some(312);
        r.metadata.candidate.version_info = Some("VersionInfo { crate_version: 0.15.18 }".into());
        r.layers.preflight = Some(PreflightReport {
            status: PreflightStatus::Ok,
            rtt_ms: summary(7.0, 15.0, 19.0, vec![]),
            version_match: None,
            network_description_match: None,
            errors: vec![],
        });
        r.layers.read_baseline = Some(ReadBaselineReport {
            per_chain: vec![PerChainReadBaseline {
                chain_id: "192907fcabc".into(),
                latency_ms: summary(
                    8.0,
                    24.0,
                    51.0,
                    vec![ErrorBucket {
                        category: "timeout".into(),
                        count: 3,
                    }],
                ),
            }],
        });
        r.layers.read_stress = Some(ReadStressReport {
            per_chain: vec![PerChainReadStress {
                chain_id: "192907fcabc".into(),
                levels: vec![StressLevel {
                    concurrency: 32,
                    duration_secs: 30,
                    completed: 27_600,
                    throughput_per_sec: 920.0,
                    latency_ms: summary(30.0, 88.0, 140.0, vec![]),
                }],
            }],
        });
        r.layers.tip_lag = Some(TipLagReport {
            per_chain: vec![PerChainTipLag {
                chain_id: "192907fcabc".into(),
                samples: vec![TipLagSample {
                    t_secs: 0,
                    candidate_tip: 12_450,
                    reference_tip: 12_500,
                    lag_blocks: 50,
                }],
                trend: TipLagTrend::Converging,
            }],
        });
        r
    }

    #[test]
    fn populated_brief_lists_key_results() {
        let b = render_brief(&populated());
        // Eyeball with: cargo test ... populated_brief_lists_key_results -- --nocapture
        println!("\n----- BRIEF -----\n{b}");
        assert!(b.contains("COMPLETE"));
        assert!(b.contains("Preflight"));
        assert!(b.contains("Read latency"));
        assert!(b.contains("Read throughput  peak 920 req/s @ conc 32"));
        assert!(b.contains("Tip lag"));
        assert!(b.contains("3 timeout"));
    }

    #[test]
    fn populated_markdown_has_summary_and_detail() {
        let md = render_markdown(&populated());
        println!("\n----- MARKDOWN -----\n{md}");
        assert!(md.contains("## Summary"));
        assert!(md.contains("| Peak sustained throughput | 920 req/s @ conc 32 |"));
        assert!(md.contains("## L1 Preflight"));
        assert!(md.contains("## L2 Read baseline"));
        assert!(md.contains("## L3 Read stress"));
        assert!(md.contains("## L5 Tip-lag"));
        assert!(md.contains("**Version:**"));
    }
}
