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
    pub ver: Cow<'static, str>,
    /// The git commit hash
    pub git: Cow<'static, str>,
    /// A hash of the RPC API
    pub rpc: Cow<'static, str>,
    /// A hash of the GraphQL API
    pub gql: Cow<'static, str>,
    /// A hash of the WIT API
    pub wit: Cow<'static, str>,
}

/// The version info of this build of Linera.
pub const VERSION_INFO: VersionInfo = VersionInfo {
    r#ver: Cow::Borrowed(env!("LINERA_VERSION_VER")),
    r#git: Cow::Borrowed(env!("LINERA_VERSION_GIT")),
    r#rpc: Cow::Borrowed(env!("LINERA_VERSION_RPC")),
    r#gql: Cow::Borrowed(env!("LINERA_VERSION_GQL")),
    r#wit: Cow::Borrowed(env!("LINERA_VERSION_WIT")),
};

impl VersionInfo {
    /// Print a human-readable listing of the version information.
    pub fn print(&self) {
        let VersionInfo {
            ver,
            git,
            rpc,
            gql,
            wit,
        } = self;

        println!(
            "\
            Linera v{ver}\n\
            Built from git commit: {git}\n\
            RPC API hash: {rpc}\n\
            GraphQL API hash: {gql}\n\
            WIT API hash: {wit}\n\
        "
        );
    }
}

impl Default for VersionInfo {
    fn default() -> Self {
        VERSION_INFO
    }
}
