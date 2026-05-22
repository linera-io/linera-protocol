// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Benchmark report data model, output-spec parsing, and serializers.

use std::path::PathBuf;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

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
            for part in item.split(|c| c == ',' || c == '+') {
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candidate {
    pub address: String,
    pub public_key: Option<String>,
    pub version_info: Option<serde_json::Value>,
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
pub type PreflightReport = serde_json::Value;
pub type ReadBaselineReport = serde_json::Value;
pub type ReadStressReport = serde_json::Value;
pub type BulkDownloadReport = serde_json::Value;
pub type TipLagReport = serde_json::Value;
pub type PartialSyncReport = serde_json::Value;

/// Render the report as a human-readable Markdown document.
///
/// Per-layer detail sections are appended by each layer's own rendering helper
/// as those layers are implemented; this base covers the metadata header.
pub fn render_markdown(r: &Report) -> String {
    use std::fmt::Write as _;
    let mut s = String::new();
    let _ = writeln!(s, "# Validator Benchmark Report");
    let _ = writeln!(s);
    let _ = writeln!(s, "- **Candidate:** `{}`", r.metadata.candidate.address);
    if let Some(pk) = &r.metadata.candidate.public_key {
        let _ = writeln!(s, "- **Public key:** `{pk}`");
    }
    let _ = writeln!(
        s,
        "- **Observer:** {} @ {}",
        r.metadata.observer.location, r.metadata.observer.hostname
    );
    let _ = writeln!(s, "- **Started:** {}", r.metadata.observer.started_at);
    if let Some(d) = r.metadata.observer.duration_secs {
        let _ = writeln!(s, "- **Duration:** {d}s");
    }
    let _ = writeln!(s, "- **Chains:** {}", r.metadata.chains_tested.join(", "));
    let _ = writeln!(s);
    s
}

/// Render a 3-5 line recap suitable for stdout when full output goes to a file.
pub fn render_brief(r: &Report) -> String {
    format!(
        "validator-benchmark | candidate={} | observer={} | chains={} | started={}\n",
        r.metadata.candidate.address,
        r.metadata.observer.location,
        r.metadata.chains_tested.len(),
        r.metadata.observer.started_at,
    )
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
}
