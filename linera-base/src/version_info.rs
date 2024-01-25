// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    async_graphql::SimpleObject,
    serde::Deserialize,
    serde::Serialize,
)]
#[non_exhaustive]
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
    pub fn print(&self) {
        let VersionInfo {
            crate_version,
            git_commit,
            rpc_hash,
            graphql_hash,
            wit_hash,
        } = self;

        println!(
            "\
            Linera v{crate_version}\n\
            Built from git commit: {git_commit}\n\
            RPC API hash: {rpc_hash}\n\
            GraphQL API hash: {graphql_hash}\n\
            WIT API hash: {wit_hash}\n\
        "
        );
    }
}

impl Default for VersionInfo {
    fn default() -> Self {
        VERSION_INFO
    }
}
