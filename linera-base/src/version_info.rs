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
    /// Whether the git worktree was dirty
    pub git_dirty: bool,
    /// A hash of the RPC API
    pub rpc_hash: Cow<'static, str>,
    /// A hash of the GraphQL API
    pub graphql_hash: Cow<'static, str>,
    /// A hash of the WIT API
    pub wit_hash: Cow<'static, str>,
}

impl VersionInfo {
    /// Print a human-readable listing of the version information.
    pub fn log(&self) {
        let VersionInfo {
            crate_version,
            git_commit,
            git_dirty,
            rpc_hash,
            graphql_hash,
            wit_hash,
        } = self;

        tracing::info!("Linera protocol: v{crate_version}");
        tracing::info!("RPC API hash: {rpc_hash}");
        tracing::info!("GraphQL API hash: {graphql_hash}");
        tracing::info!("WIT API hash: {wit_hash}");
        tracing::info!(
            "Source code: https://github.com/linera-io/linera-protocol/commit/{git_commit}{}",
            if *git_dirty { " (dirty)" } else { "" },
        );
    }

    /// Returns true if `other` is probably incompatible with `self`. Currently, the
    /// commit hash of the source code is the only field that can differ.
    pub fn is_probably_incompatible_with(&self, other: &Self) -> bool {
        (
            &self.crate_version,
            &self.rpc_hash,
            &self.graphql_hash,
            &self.wit_hash,
        ) != (
            &other.crate_version,
            &other.rpc_hash,
            &other.graphql_hash,
            &other.wit_hash,
        )
    }

    /// A static string corresponding to `VersionInfo::default().to_string()`.
    pub fn default_str() -> &'static str {
        static STRING: Lazy<String> = Lazy::new(|| VersionInfo::default().to_string());
        STRING.as_str()
    }
}

impl std::fmt::Display for VersionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let VersionInfo {
            crate_version,
            git_commit,
            git_dirty,
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
RPC API hash: {rpc_hash}
GraphQL API hash: {graphql_hash}
WIT API hash: {wit_hash}
Source code: https://github.com/linera-io/linera-protocol/commit/{git_commit}{}
",
            if *git_dirty { " (dirty)" } else { "" },
        )
    }
}

impl Default for VersionInfo {
    fn default() -> Self {
        #![allow(clippy::eq_op)]
        Self {
            crate_version: Cow::Borrowed(env!("CARGO_PKG_VERSION")),
            git_commit: Cow::Borrowed(env!("LINERA_VERSION_GIT_COMMIT")),
            git_dirty: env!("LINERA_VERSION_GIT_DIRTY") == "true",
            rpc_hash: Cow::Borrowed(env!("LINERA_VERSION_RPC_HASH")),
            graphql_hash: Cow::Borrowed(env!("LINERA_VERSION_GRAPHQL_HASH")),
            wit_hash: Cow::Borrowed(env!("LINERA_VERSION_WIT_HASH")),
        }
    }
}
