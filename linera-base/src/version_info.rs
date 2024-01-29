// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::sync::Lazy;
use async_graphql::SimpleObject;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

const VERSION_SCRIPT: &str = include_str!("../versions.sh");

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
        tracing::info!("RPC API hash: {rpc_hash}");
        tracing::info!("GraphQL API hash: {graphql_hash}");
        tracing::info!("WIT API hash: {wit_hash}");
        tracing::info!(
            "Source code: https://github.com/linera-io/linera-protocol/commit/{git_commit}"
        );
    }

    /// Dynamically get the version info corresponding to the `linera-protocol` checkout
    /// in the current working directory.
    pub fn get() -> std::io::Result<Self> {
        use std::io::{Read as _, Write as _};

        let mut child = std::process::Command::new("bash")
            .stdout(std::process::Stdio::piped())
            .stdin(std::process::Stdio::piped())
            .spawn()?;

        write!(child.stdin.take().unwrap(), "{}", VERSION_SCRIPT)?;

        let mut output = String::new();
        child.stdout.take().unwrap().read_to_string(&mut output)?;

        Ok(envy::from_iter(output.lines().map(|line| {
            let (key, value) = line
                .split_once('=')
                .expect("output of version info script is not a key/value pair");
            (key.to_owned(), value.to_owned())
        }))
        .expect("failed to parse result of version info script"))
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
Source code: https://github.com/linera-io/linera-protocol/commit/{git_commit}
"
        )
    }
}

impl Default for VersionInfo {
    fn default() -> Self {
        VERSION_INFO
    }
}
