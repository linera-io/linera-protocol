// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod client;
mod conversions;
pub(crate) mod indexer_exporter;

/// The gRPC client and message types generated from the indexer proto definitions.
pub mod indexer_api {
    // Generated gRPC bindings; the generated items cannot carry doc comments.
    #![allow(missing_docs)]
    tonic::include_proto!("indexer.linera_indexer");
}
