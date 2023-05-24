// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, bail, Result};
use std::{
    ffi::OsStr,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};
use tracing::debug;

const CARGO_TOML: &str = r#"
[package]
name = "{project-name}"
version = "0.1.0"
edition = "2021"

[dependencies]
async-trait = "0.1.52"
bcs = "0.1.3"
futures = "0.3.17"
{linera-sdk-dep}
serde = { version = "1.0.130", features = ["derive"] }
serde_json = "1.0.93"
thiserror = "1.0.31"

[dev-dependencies]
{linera-sdk-dev-dep}
webassembly-test = "0.1.0"

[[bin]]
name = "{project-name}_contract"
path = "src/contract.rs"

[[bin]]
name = "{project-name}_service"
path = "src/service.rs"
"#;

const STATE: &str = r#"
use serde::{Deserialize, Serialize};

/// The application state.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct State {
    // Add fields here.
}
"#;

const LIB: &str = r#"
use linera_sdk::base::{ContractAbi, ServiceAbi};

pub struct StateAbi;

impl ContractAbi for StateAbi {
    type InitializationArgument = ();
    type Parameters = ();
    type Operation = ();
    type ApplicationCall = ();
    type Effect = ();
    type SessionCall = ();
    type Response = ();
    type SessionState = ();
}

impl ServiceAbi for StateAbi {
    type Query = ();
    type QueryResponse = ();
    type Parameters = ();
}
"#;

const CONTRACT: &str = r#"
#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::State;
use async_trait::async_trait;
use linera_sdk::{
    base::{SessionId, WithContractAbi},
    ApplicationCallResult, CalleeContext, Contract, EffectContext,
    ExecutionResult, OperationContext, SessionCallResult, SimpleStateStorage,
};
use thiserror::Error;

linera_sdk::contract!(State);

impl WithContractAbi for State {
    type Abi = {project-name}::StateAbi;
}

#[async_trait]
impl Contract for State {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        _argument: (),
    ) -> Result<ExecutionResult<Self::Effect>, Self::Error> {
        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        _operation: (),
    ) -> Result<ExecutionResult<Self::Effect>, Self::Error> {
        Ok(ExecutionResult::default())
    }

    async fn execute_effect(
        &mut self,
        _context: &EffectContext,
        _effect: (),
    ) -> Result<ExecutionResult<Self::Effect>, Self::Error> {
        Ok(ExecutionResult::default())
    }

    async fn handle_application_call(
        &mut self,
        _context: &CalleeContext,
        _argument: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult<Self::Effect, Self::Response, Self::SessionState>, Self::Error> {
        Ok(ApplicationCallResult::default())
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        _session: (),
        _argument: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult<Self::Effect, Self::Response, Self::SessionState>, Self::Error> {
        Ok(SessionCallResult::default())
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),

    // Add more error variants here.
}
"#;

const SERVICE: &str = r#"
#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::State;
use async_trait::async_trait;
use linera_sdk::{base::WithServiceAbi, QueryContext, Service, SimpleStateStorage};
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(State);

impl WithServiceAbi for State {
    type Abi = {project-name}::StateAbi;
}

#[async_trait]
impl Service for State {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        _argument: (),
    ) -> Result<(), Self::Error> {
        Err(Error::QueriesNotSupported)
    }
}

/// An error that can occur while querying the service.
#[derive(Debug, Error)]
pub enum Error {
    /// Query not supported by the application.
    #[error("Queries not supported by application")]
    QueriesNotSupported,

    /// Invalid query argument; could not deserialize request.
    #[error("Invalid query argument; could not deserialize request")]
    InvalidQuery(#[from] serde_json::Error),

    // Add error variants here.
}
"#;

const CONFIG: &str = r#"
[build]
target = "wasm32-unknown-unknown"
"#;

pub struct Project {
    root: PathBuf,
}

impl Project {
    pub fn new(root: PathBuf) -> Result<Self> {
        if root.exists() {
            bail!("destination {} already exists", root.display());
        }
        if root.extension().is_some() {
            bail!("project name must be a directory");
        }
        debug!("creating directory at {}", root.display());
        std::fs::create_dir_all(&root)?;

        debug!("creating the source directory");
        let source_directory = Self::create_source_directory(&root)?;

        let project_name = root
            .file_name()
            .and_then(OsStr::to_str)
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("path specified cannot terminate in . or .."))?;

        debug!("writing Cargo.toml");
        Self::create_cargo_toml(&root, &project_name)?;

        debug!("writing state.rs");
        Self::create_state_file(&source_directory)?;

        debug!("writing lib.rs");
        Self::create_lib_file(&source_directory)?;

        debug!("writing contract.rs");
        Self::create_contract_file(&source_directory, &project_name)?;

        debug!("writing service.rs");
        Self::create_service_file(&source_directory, &project_name)?;

        debug!("creating cargo config");
        Self::create_cargo_config(&root)?;

        Ok(Self { root })
    }

    pub fn from_existing_project(root: PathBuf) -> Result<Self> {
        if !root.exists() {
            bail!("could not find project at {}", root.display());
        }
        Ok(Self { root })
    }

    pub fn test(&self) -> Result<()> {
        if !Self::runner_is_installed()? {
            debug!("Linera test runner not found");
            Self::install_test_runner()?;
        }
        let cargo_test = Command::new("cargo")
            .env(
                "CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER",
                Self::runner_path()?.display().to_string().as_str(),
            )
            .arg("test")
            .args(["--target", "wasm32-unknown-unknown"])
            .current_dir(&self.root)
            .spawn()?
            .wait()?;
        if !cargo_test.success() {
            bail!("tests failed")
        }
        Ok(())
    }

    pub fn runner_is_installed() -> Result<bool> {
        Ok(Self::runner_path()?.exists())
    }

    fn install_test_runner() -> Result<()> {
        println!("installing test runner...");
        let cargo_install = Command::new("cargo")
            .args(["install", "linera-test-runner"])
            .spawn()?
            .wait()?;
        if !cargo_install.success() {
            bail!("failed to install linera-test-runner")
        }
        Ok(())
    }

    fn runner_path() -> Result<PathBuf> {
        Self::cargo_home().map(|cargo_home| cargo_home.join("bin").join("linera-test-runner"))
    }

    fn cargo_home() -> Result<PathBuf> {
        if let Ok(cargo_home) = std::env::var("CARGO_HOME") {
            Ok(PathBuf::from(cargo_home))
        } else if let Some(home) = dirs::home_dir() {
            Ok(home.join(".cargo"))
        } else {
            bail!("could not find CARGO_HOME directory, please specify it explicitly")
        }
    }

    fn create_source_directory(project_root: &Path) -> Result<PathBuf> {
        let source_directory = project_root.join("src");
        std::fs::create_dir(&source_directory)?;
        Ok(source_directory)
    }

    fn create_cargo_toml(project_root: &Path, project_name: &str) -> Result<()> {
        let toml_path = project_root.join("Cargo.toml");
        let toml_contents = CARGO_TOML.replace("{project-name}", project_name);
        let toml_contents = Self::add_linera_sdk_dependency(&toml_contents);
        Self::write_string_to_file(&toml_path, &toml_contents)
    }

    fn create_state_file(source_directory: &Path) -> Result<()> {
        let state_path = source_directory.join("state.rs");
        Self::write_string_to_file(&state_path, STATE)
    }

    fn create_lib_file(source_directory: &Path) -> Result<()> {
        let state_path = source_directory.join("lib.rs");
        Self::write_string_to_file(&state_path, LIB)
    }

    fn create_contract_file(source_directory: &Path, project_name: &str) -> Result<()> {
        let project_name = project_name.replace('-', "_");
        let contract_path = source_directory.join("contract.rs");
        let contract_contents = CONTRACT.replace("{project-name}", &project_name);
        Self::write_string_to_file(&contract_path, &contract_contents)
    }

    fn create_service_file(source_directory: &Path, project_name: &str) -> Result<()> {
        let project_name = project_name.replace('-', "_");
        let service_path = source_directory.join("service.rs");
        let service_contents = SERVICE.replace("{project-name}", &project_name);
        Self::write_string_to_file(&service_path, &service_contents)
    }

    fn create_cargo_config(project_root: &Path) -> Result<()> {
        let config_dir_path = project_root.join(".cargo");
        let config_file_path = config_dir_path.join("config.toml");
        std::fs::create_dir(&config_dir_path)?;
        Self::write_string_to_file(&config_file_path, CONFIG)
    }

    fn write_string_to_file(path: &Path, content: &str) -> Result<()> {
        let mut file = File::create(path)?;
        file.write_all(content.as_bytes())?;
        Ok(())
    }

    /// Adds ['linera-sdk'] dependencies in `debug` mode.
    ///
    /// Uses the directory of `linera-service` at compile time to figure out
    /// where `linera-sdk` is.
    #[cfg(debug_assertions)]
    fn add_linera_sdk_dependency(cargo_toml: &str) -> String {
        let linera_service_path: PathBuf = env!("CARGO_MANIFEST_DIR")
            .parse()
            .expect("the CARGO_MANIFEST_DIR should always be a valid path");
        let linera_sdk_path = linera_service_path
            .join("..")
            .join("linera-sdk")
            .canonicalize()
            .expect("the linera-sdk crate should always exist");
        let linera_sdk_dep = format!(
            "linera-sdk = {{ path = \"{}\" }}",
            linera_sdk_path.display()
        );
        let with_sdk_dependency = cargo_toml.replace("{linera-sdk-dep}", &linera_sdk_dep);
        let linera_sdk_dev_dep = format!(
            "linera-sdk = {{ path = \"{}\", features = [\"test\"] }}",
            linera_sdk_path.display()
        );
        with_sdk_dependency.replace("{linera-sdk-dev-dep}", &linera_sdk_dev_dep)
    }

    /// Adds ['linera-sdk'] dependencies in `release` mode.
    ///
    /// Includes the contents of `linera-sdk`'s `Cargo.toml` at compile time
    /// to figure out the latest version.
    #[cfg(not(debug_assertions))]
    fn add_linera_sdk_dependency(cargo_toml: &str) -> String {
        let content = include_str!("../../linera-sdk/Cargo.toml");
        let sdk_cargo_toml: toml::Value = toml::from_str(content)
            .expect("there was an error parsing a TOML file included at compile-time - this should never happen.");
        let version = sdk_cargo_toml["package"]["version"].as_str()
            .expect("there was an error finding the version in a TOML file included at compile-time - this should never happen.");
        let with_sdk_dependency =
            cargo_toml.replace("{linera-sdk-dep}", &format!("linera-sdk = \"{}\"", version));
        let with_sdk_dev_dependency = with_sdk_dependency.replace(
            "{linera-sdk-dev-dep}",
            &format!(
                "linera-sdk = {{ version = \"{}\", features = [\"test\"] }}",
                version
            ),
        );
        with_sdk_dev_dependency
    }
}
