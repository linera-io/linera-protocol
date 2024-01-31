// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Cow,
    io::{Read as _, Write as _},
    path::PathBuf,
};

#[cfg_attr(linera_version_building, derive(async_graphql::SimpleObject, serde::Deserialize, serde::Serialize))]
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
)]
/// The version info of a build of Linera.
pub struct VersionInfo {
    /// The crate version
    pub crate_version: Cow<'static, str>,
    /// The git commit hash
    pub git_commit: Cow<'static, str>,
    /// Whether the git checkout was dirty
    pub git_dirty: bool,
    /// A hash of the RPC API
    pub rpc_hash: Cow<'static, str>,
    /// A hash of the GraphQL API
    pub graphql_hash: Cow<'static, str>,
    /// A hash of the WIT API
    pub wit_hash: Cow<'static, str>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to interpret cargo-metadata: {0}")]
    CargoMetadata(#[from] cargo_metadata::Error),
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

pub type Result<T> = std::result::Result<T, Error>;

fn get_hash(relevant_paths: &mut Vec<PathBuf>, glob: &str) -> Result<String> {
    use base64::engine::{general_purpose::STANDARD_NO_PAD, Engine as _};
    use sha3::Digest as _;

    let mut hasher = sha3::Sha3_256::new();
    let mut buffer = [0u8; 4096];

    for path in glob::glob(glob)? {
        let path = path?;
        let mut file = std::fs::File::open(&path)?;
        relevant_paths.push(path);
        while file.read(&mut buffer)? != 0 {
            hasher.update(buffer);
        }
    }

    Ok(STANDARD_NO_PAD.encode(hasher.finalize()))
}

fn run<'a>(cmd: &str, args: &[&str], stdin: impl Into<Option<&'a str>>) -> Result<Outcome> {
    let stdin = stdin.into();

    let mut cmd = std::process::Command::new(cmd);

    if stdin.is_some() {
        cmd.stdin(std::process::Stdio::piped());
    }

    let mut child = cmd
        .args(args)
        .stdout(std::process::Stdio::piped())
        .spawn()?;

    if let Some(stdin) = stdin {
        write!(child.stdin.take().unwrap(), "{}", stdin)?;
    }

    let mut output = String::new();
    child.stdout.take().unwrap().read_to_string(&mut output)?;

    Ok(Outcome {
        status: child.wait()?,
        output,
    })
}

impl VersionInfo {
    pub fn get() -> Result<Self> {
        Self::trace_get(&mut vec![])
    }

    fn trace_get(paths: &mut Vec<PathBuf>) -> Result<Self> {
        let metadata = cargo_metadata::MetadataCommand::new().no_deps().exec()?;
        let workspace = metadata.workspace_root.as_std_path().display();

        Ok(Self {
            crate_version: metadata
                .packages
                .iter()
                .filter_map(|x| {
                    if x.name == "linera-version" {
                        Some(x.version.to_string())
                    } else {
                        None
                    }
                })
                .next()
                .expect("no `linera-version` package found")
                .into(),
            git_commit: run("git", &["rev-parse", "HEAD"], None)?.output[..10]
                .to_owned()
                .into(),
            git_dirty: !run("git", &["diff-index", "--quiet", "HEAD"], None)?
                .status
                .success(),
            rpc_hash: get_hash(
                paths,
                &format!("{workspace}/linera-rpc/tests/staged/formats.yaml"),
            )?
            .into(),
            graphql_hash: get_hash(
                paths,
                &format!("{workspace}/linera-service-graphql-client/gql/*.graphql"),
            )?
            .into(),
            wit_hash: get_hash(paths, &format!("{workspace}/linera-sdk/*.wit"))?.into(),
        })
    }
}
