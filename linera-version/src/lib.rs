// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod version_info;
pub use version_info::*;

use std::borrow::Cow;

pub static VERSION_INFO: VersionInfo = VersionInfo {
    crate_version: Cow::Borrowed(env!("LINERA_VERSION_CRATE_VERSION")),
    git_commit: Cow::Borrowed(env!("LINERA_VERSION_GIT_COMMIT")),
    #[cfg(linera_version_git_dirty)]
    git_dirty: true,
    #[cfg(not(linera_version_git_dirty))]
    git_dirty: false,
    rpc_hash: Cow::Borrowed(env!("LINERA_VERSION_RPC_HASH")),
    graphql_hash: Cow::Borrowed(env!("LINERA_VERSION_GRAPHQL_HASH")),
    wit_hash: Cow::Borrowed(env!("LINERA_VERSION_WIT_HASH")),
};

impl std::fmt::Display for VersionInfo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "\n\
            Linera protocol: v{crate_version}\n\
            RPC API hash: {rpc_hash}\n\
            GraphQL API hash: {graphql_hash}\n\
            WIT API hash: {wit_hash}\n\
            Source code: https://github.com/linera-io/linera-protocol/commit/{git_commit}{git_dirty}\n\
            ",
            crate_version=self.crate_version,
            rpc_hash=self.rpc_hash,
            graphql_hash=self.graphql_hash,
            wit_hash=self.wit_hash,
            git_commit=self.git_commit,
            git_dirty=if self.git_dirty {
                " (dirty)"
            } else {
                ""
            }
        )
    }
}

impl VersionInfo {
    /// Print a human-readable listing of the version information at `info` level.
    pub fn log(&self) {
        for line in format!("{self}").lines() {
            tracing::info!("{line}");
        }
    }

    /// A static string corresponding to `VersionInfo::default().to_string()`.
    pub fn default_str() -> &'static str {
        use once_cell::sync::Lazy;
        static STRING: Lazy<String> = Lazy::new(|| VersionInfo::default().to_string());
        STRING.as_str()
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
}

impl Default for VersionInfo {
    fn default() -> Self {
        VERSION_INFO.clone()
    }
}
