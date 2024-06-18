// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io::Read as _, path::PathBuf};

#[cfg(linera_version_building)]
use crate::serde_pretty::Pretty;

#[cfg_attr(linera_version_building, derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CrateVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl From<semver::Version> for CrateVersion {
    fn from(
        semver::Version {
            major,
            minor,
            patch,
            ..
        }: semver::Version,
    ) -> Self {
        Self {
            major: major as u32,
            minor: minor as u32,
            patch: patch as u32,
        }
    }
}

impl From<CrateVersion> for semver::Version {
    fn from(
        CrateVersion {
            major,
            minor,
            patch,
        }: CrateVersion,
    ) -> Self {
        Self::new(major as u64, minor as u64, patch as u64)
    }
}

pub type Hash = std::borrow::Cow<'static, str>;

#[cfg_attr(
    linera_version_building,
    derive(async_graphql::SimpleObject, serde::Deserialize, serde::Serialize)
)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// The version info of a build of Linera.
pub struct VersionInfo {
    /// The crate version
    pub crate_version: Pretty<CrateVersion, semver::Version>,
    /// The git commit hash
    pub git_commit: Hash,
    /// Whether the git checkout was dirty
    pub git_dirty: bool,
    /// A hash of the RPC API
    pub rpc_hash: Hash,
    /// A hash of the GraphQL API
    pub graphql_hash: Hash,
    /// A hash of the WIT API
    pub wit_hash: Hash,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to interpret cargo-metadata: {0}")]
    CargoMetadata(#[from] cargo_metadata::Error),
    #[error("no such package: {0}")]
    NoSuchPackage(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("glob error: {0}")]
    Glob(#[from] glob::GlobError),
    #[error("pattern error: {0}")]
    Pattern(#[from] glob::PatternError),
}

struct Outcome {
    status: std::process::ExitStatus,
    output: String,
}

fn get_hash(
    relevant_paths: &mut Vec<PathBuf>,
    metadata: &cargo_metadata::Metadata,
    package: &str,
    glob: &str,
) -> Result<String, Error> {
    use base64::engine::{general_purpose::STANDARD_NO_PAD, Engine as _};
    use sha3::Digest as _;

    let Some(package_root) = get_package_root(metadata, package) else {
        return Ok("package not used".to_owned());
    };

    let mut hasher = sha3::Sha3_256::new();
    let mut buffer = [0u8; 4096];

    let package_glob = format!("{}/{}", package_root.display(), glob);

    let mut n_file = 0;
    for path in glob::glob(&package_glob)? {
        let path = path?;
        let mut file = std::fs::File::open(&path)?;
        relevant_paths.push(path);
        n_file += 1;
        while file.read(&mut buffer)? != 0 {
            hasher.update(buffer);
        }
    }
    assert!(n_file > 0);

    Ok(STANDARD_NO_PAD.encode(hasher.finalize()))
}

fn run(cmd: &str, args: &[&str]) -> Result<Outcome, Error> {
    let mut cmd = std::process::Command::new(cmd);

    let mut child = cmd
        .args(args)
        .stdout(std::process::Stdio::piped())
        .spawn()?;

    let mut output = String::new();
    child.stdout.take().unwrap().read_to_string(&mut output)?;

    Ok(Outcome {
        status: child.wait()?,
        output,
    })
}

fn get_package<'r>(
    metadata: &'r cargo_metadata::Metadata,
    package_name: &str,
) -> Option<&'r cargo_metadata::Package> {
    metadata.packages.iter().find(|p| p.name == package_name)
}

fn get_package_root<'r>(
    metadata: &'r cargo_metadata::Metadata,
    package_name: &str,
) -> Option<&'r std::path::Path> {
    Some(
        get_package(metadata, package_name)?
            .targets
            .first()
            .expect("package must have at least one target")
            .src_path
            .ancestors()
            .find(|p| p.join("Cargo.toml").exists())
            .expect("package should have a Cargo.toml")
            .as_std_path(),
    )
}

impl VersionInfo {
    pub fn get() -> Result<Self, Error> {
        Self::trace_get(&std::env::current_dir()?, &mut vec![])
    }

    fn trace_get(crate_dir: &std::path::Path, paths: &mut Vec<PathBuf>) -> Result<Self, Error> {
        let metadata = cargo_metadata::MetadataCommand::new()
            .current_dir(crate_dir)
            .exec()?;

        let crate_version = Pretty::new(
            get_package(&metadata, env!("CARGO_PKG_NAME"))
                .expect("this package must be in the dependency tree")
                .version
                .clone()
                .into(),
        );

        let mut git_dirty = false;
        let git_commit = if let Ok(git_commit) = std::env::var("GIT_COMMIT") {
            git_commit
        } else {
            let git_outcome = run("git", &["rev-parse", "HEAD"])?;
            if git_outcome.status.success() {
                git_dirty = run("git", &["diff-index", "--quiet", "HEAD"])?
                    .status
                    .code()
                    == Some(1);
                git_outcome.output[..10].to_owned()
            } else {
                format!("v{}", crate_version)
            }
        }
        .into();

        let rpc_hash = get_hash(
            paths,
            &metadata,
            "linera-rpc",
            "tests/snapshots/format__format.yaml.snap",
        )?
        .into();

        let graphql_hash = get_hash(
            paths,
            &metadata,
            "linera-service-graphql-client",
            "gql/*.graphql",
        )?
        .into();

        let wit_hash = get_hash(paths, &metadata, "linera-sdk", "wit/*.wit")?.into();

        Ok(Self {
            crate_version,
            git_commit,
            git_dirty,
            rpc_hash,
            graphql_hash,
            wit_hash,
        })
    }
}
