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
futures = "0.3.17"
linera-sdk = { path = "../../linera-sdk" }
linera-views = { path = "../../linera-views" }
serde = { version = "1.0.130", features = ["derive"] }
thiserror = "1.0.31"

[dev-dependencies]
linera-sdk = { path = "../../linera-sdk", features = ["test"] }
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

const CONTRACT: &str = r#"
#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::State;
use async_trait::async_trait;
use linera_sdk::{
    base::SessionId, ApplicationCallResult, CalleeContext, Contract, EffectContext,
    ExecutionResult, OperationContext, Session, SessionCallResult, SimpleStateStorage,
};
use thiserror::Error;

linera_sdk::contract!(State);

#[async_trait]
impl Contract for State {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        _argument: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        _operation: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        Ok(ExecutionResult::default())
    }

    async fn execute_effect(
        &mut self,
        _context: &EffectContext,
        _effect: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        Ok(ExecutionResult::default())
    }

    async fn handle_application_call(
        &mut self,
        _context: &CalleeContext,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error> {
        Ok(ApplicationCallResult::default())
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        _session: Session,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, Self::Error> {
        Ok(SessionCallResult::default())
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    // Add error variants here.
}
"#;

const SERVICE: &str = r#"
#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::State;
use async_trait::async_trait;
use linera_sdk::{QueryContext, Service, SimpleStateStorage};
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(State);

#[async_trait]
impl Service for State {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        _argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        Err(Error::QueriesNotSupported)
    }
}

/// An error that can occur while querying the service.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum Error {
    /// Add error variants here.
    #[error("Queries not supported by application")]
    QueriesNotSupported
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

        debug!("writing Cargo.toml");
        Self::create_cargo_toml(&root)?;

        debug!("writing state.rs");
        Self::create_state_file(&source_directory)?;

        debug!("writing contract.rs");
        Self::create_contract_file(&source_directory)?;

        debug!("writing service.rs");
        Self::create_service_file(&source_directory)?;

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

    fn create_cargo_toml(project_root: &Path) -> Result<()> {
        let project_name = project_root
            .file_name()
            .and_then(OsStr::to_str)
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("path specified cannot terminate in . or .."))?;
        let toml_path = project_root.join("Cargo.toml");
        let toml_contents = CARGO_TOML.replace("{project-name}", &project_name);
        Self::write_string_to_file(&toml_path, &toml_contents)
    }

    fn create_state_file(source_directory: &Path) -> Result<()> {
        let state_path = source_directory.join("state.rs");
        Self::write_string_to_file(&state_path, STATE)
    }

    fn create_contract_file(source_directory: &Path) -> Result<()> {
        let contract_path = source_directory.join("contract.rs");
        Self::write_string_to_file(&contract_path, CONTRACT)
    }

    fn create_service_file(source_directory: &Path) -> Result<()> {
        let service_path = source_directory.join("service.rs");
        Self::write_string_to_file(&service_path, SERVICE)
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
}
