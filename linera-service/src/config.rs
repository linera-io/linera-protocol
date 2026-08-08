// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub use linera_exporter::config::{
    BlockExporterConfig, Destination, DestinationConfig, DestinationId, DestinationKind,
    LimitsConfig,
};

/// How a shard reaches the other validators when exporting the blocks it executes.
///
/// The choice is a trade-off between this validator's own proxy load and keeping shards off the
/// internet, and it is deliberately an operator's call: the cost of relaying every exported block
/// through the proxy is not something we can predict from the code.
///
/// Note that neither setting changes what the *receiving* validator's proxy has to absorb — it
/// serves the same requests either way. Relaying does, however, concentrate them: a peer sees
/// connections from this validator's handful of proxies rather than from every one of its shards.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum BlockExportTransport {
    /// Send through this validator's own proxy, which forwards to the destination. Shards need no
    /// outbound access, which matters because a shard holds the validator secret key.
    Relay,
    /// Send straight from the shard to the destination validator's public endpoint. Takes the
    /// sending proxy out of the path entirely, at the cost of giving shards — which hold the
    /// validator secret key — outbound access to the other validators.
    Direct,
}
