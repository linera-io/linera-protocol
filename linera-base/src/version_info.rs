// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::sync::Lazy;
use async_graphql::SimpleObject;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq, Eq, Hash, SimpleObject, Deserialize, Serialize)]
/// The version info of a build of Linera.
pub struct VersionInfo {
    /// The crate version
    pub crate_version: Cow<'static, str>,
    /// The git commit hash
    pub git_commit: Cow<'static, str>,
    /// A hash of the RPC API
    pub rpc_hash: Cow<'static, str>,
    /// A hash of the GraphQL API
    pub graphql_hash: Cow<'static, str>,
    /// A hash of the WIT API
    pub wit_hash: Cow<'static, str>,
}

/// The version info of this build of Linera.
pub const VERSION_INFO: VersionInfo = VersionInfo {
    crate_version: Cow::Borrowed(env!("CARGO_PKG_VERSION")),
    git_commit: Cow::Borrowed(env!("LINERA_VERSION_GIT_COMMIT")),
    rpc_hash: Cow::Borrowed(env!("LINERA_VERSION_RPC_HASH")),
    graphql_hash: Cow::Borrowed(env!("LINERA_VERSION_GRAPHQL_HASH")),
    wit_hash: Cow::Borrowed(env!("LINERA_VERSION_WIT_HASH")),
};

impl VersionInfo {
    /// Print a human-readable listing of the version information.
    pub fn log(&self) {
        let VersionInfo {
            crate_version,
            git_commit,
            rpc_hash,
            graphql_hash,
            wit_hash,
        } = self;

        tracing::info!("Linera protocol: v{crate_version}");
        tracing::info!("Built from git commit: {git_commit}");
        tracing::info!("RPC API hash: {rpc_hash}");
        tracing::info!("GraphQL API hash: {graphql_hash}");
        tracing::info!("WIT API hash: {wit_hash}");
    }

    /// A static string corresponding to `VersionInfo::default().to_string()`.
    pub fn default_str() -> &'static str {
        static STRING: Lazy<String> = Lazy::new(|| VERSION_INFO.to_string());
        STRING.as_str()
    }
}

impl std::fmt::Display for VersionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let VersionInfo {
            crate_version,
            git_commit,
            rpc_hash,
            graphql_hash,
            wit_hash,
        } = self;

        // Starting with a new line is convenient for the clap annotation:
        // `#[command(version = linera_base::VersionInfo::default_str())]`
        write!(
            f,
            "
Linera protocol: v{crate_version}
Built from git commit: {git_commit}
RPC API hash: {rpc_hash}
GraphQL API hash: {graphql_hash}
WIT API hash: {wit_hash}
"
        )
    }
}

impl Default for VersionInfo {
    fn default() -> Self {
        VERSION_INFO
    }
}
