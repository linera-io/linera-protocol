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
}
