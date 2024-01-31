// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod version_info;
pub use version_info::*;

pub static VERSION_INFO: VersionInfo = include!(env!("LINERA_VERSION_STATIC_PATH"));

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

impl CrateVersion {
    /// Whether this version is known to be API-compatible with `other`.
    /// Note that this relation _is not_ symmetric.
    pub fn is_compatible_with(&self, other: &CrateVersion) -> bool {
        if self.major == 0 {
            // Cargo conventions decree that if the major version is 0, minor versions
            // denote backwards-incompatible changes and patch versions denote
            // backwards-compatible changes.
            self.minor == other.minor && self.patch <= other.patch
        } else {
            self.major == other.major && self.minor <= other.minor
        }
    }
}

impl std::fmt::Display for CrateVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)
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

    fn api_hashes(&self) -> (&Hash, &Hash, &Hash) {
        (&self.rpc_hash, &self.graphql_hash, &self.wit_hash)
    }

    /// Whether this version is known to be (remote!) API-compatible with `other`.
    /// Note that this relation _is not_ symmetric.
    /// It also may give false negatives.
    pub fn is_compatible_with(&self, other: &VersionInfo) -> bool {
        self.api_hashes() == other.api_hashes()
            || self.crate_version.is_compatible_with(&other.crate_version)
    }
}

impl Default for VersionInfo {
    fn default() -> Self {
        VERSION_INFO.clone()
    }
}
