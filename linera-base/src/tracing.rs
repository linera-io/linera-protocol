// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides unified handling for tracing subscribers within Linera binaries.

use is_terminal::IsTerminal as _;
use tracing_subscriber::fmt::format::FmtSpan;

fn fmt_span_from_str(events: &str) -> FmtSpan {
    let mut fmt_span = FmtSpan::NONE;
    for event in events.split(',') {
        fmt_span |= match event {
            "new" => FmtSpan::NEW,
            "enter" => FmtSpan::ENTER,
            "exit" => FmtSpan::EXIT,
            "close" => FmtSpan::CLOSE,
            "active" => FmtSpan::ACTIVE,
            "full" => FmtSpan::FULL,
            _ => FmtSpan::NONE,
        };
    }
    fmt_span
}

/// Initializes tracing in a standard way.
/// The environment variables `RUST_LOG`, `RUST_LOG_SPAN_EVENTS`, and `RUST_LOG_FORMAT`
/// can be used to control the verbosity, the span event verbosity, and the output format,
/// respectively.
pub fn init() {
    let span_events = std::env::var("RUST_LOG_SPAN_EVENTS")
        .ok()
        .map(|s| fmt_span_from_str(&s))
        .unwrap_or(FmtSpan::NONE);

    let mut subscriber = tracing_subscriber::fmt()
        .with_span_events(span_events)
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        );

    if std::env::var("NO_COLOR").is_ok_and(|x| !x.is_empty())
        || cfg!(feature = "web")
        || !std::io::stderr().is_terminal()
    {
        subscriber = subscriber.with_ansi(false);
    }

    let format = std::env::var("RUST_LOG_FORMAT").unwrap_or("plain".to_string());
    match format.as_str() {
        "json" => subscriber.json().init(),
        "pretty" => subscriber.pretty().init(),
        "plain" => subscriber.init(),
        format => {
            panic!("Invalid RUST_LOG_FORMAT: `{format}`.  Valid values are `json` or `pretty`.")
        }
    }
}
