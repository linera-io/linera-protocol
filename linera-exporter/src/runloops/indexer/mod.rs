// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod client;
mod conversions;
pub(crate) mod indexer_exporter;

pub mod indexer_api {
    tonic::include_proto!("indexer.linera_indexer");
}
