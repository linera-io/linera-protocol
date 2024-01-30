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
    git_commit: Cow::Borrowed(include_str!("../git_commit.txt")),
    rpc_hash: Cow::Borrowed(include_str!("../rpc_hash.txt")),
    graphql_hash: Cow::Borrowed(include_str!("../graphql_hash.txt")),
    wit_hash: Cow::Borrowed(include_str!("../wit_hash.txt")),
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
        tracing::info!("RPC API hash: {rpc_hash}");
        tracing::info!("GraphQL API hash: {graphql_hash}");
        tracing::info!("WIT API hash: {wit_hash}");
        tracing::info!(
            "Source code: https://github.com/linera-io/linera-protocol/tree/{git_commit}"
        );
    }

    fn crate_version_without_patch(&self) -> String {
        let parts = self.crate_version.split('.').collect::<Vec<_>>();
        if parts.len() != 3 {
            return self.crate_version.to_string();
        }
        format!("{}.{}", parts[0], parts[1])
    }

    /// Returns true if `other` is probably incompatible with `self`. Currently, the
    /// commit hash of the source code is the only field that can differ.
    pub fn is_probably_incompatible_with(&self, other: &Self) -> bool {
        (
            self.crate_version_without_patch(),
            &self.rpc_hash,
            &self.graphql_hash,
            &self.wit_hash,
        ) != (
            other.crate_version_without_patch(),
            &other.rpc_hash,
            &other.graphql_hash,
            &other.wit_hash,
        )
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
RPC API hash: {rpc_hash}
GraphQL API hash: {graphql_hash}
WIT API hash: {wit_hash}
Source code: https://github.com/linera-io/linera-protocol/tree/{git_commit}
"
        )
    }
}

impl Default for VersionInfo {
    fn default() -> Self {
        VERSION_INFO
    }
}

#[test]
fn test_basic_semver() {
    let mut v1 = VERSION_INFO.clone();
    let mut v2 = VERSION_INFO.clone();
    v1.crate_version = "1.2.0".into();
    v2.crate_version = "1.2.1".into();
    assert!(!v1.is_probably_incompatible_with(&v2));

    v2.crate_version = "1.3.0".into();
    assert!(v1.is_probably_incompatible_with(&v2));
}
