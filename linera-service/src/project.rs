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
use tracing::{debug, info};

pub struct Project {
    root: PathBuf,
}

const RUNNER_BIN_NAME: &str = "test-runner";

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
        info!("installing test runner...");
        let cargo_install = Command::new("cargo")
            .args(["install", "linera-sdk"])
            .args(["--bin", RUNNER_BIN_NAME])
            .spawn()?
            .wait()?;
        if !cargo_install.success() {
            bail!("failed to install {}", &RUNNER_BIN_NAME)
        }
        Ok(())
    }

    fn runner_path() -> Result<PathBuf> {
        Self::cargo_home().map(|cargo_home| cargo_home.join("bin").join(RUNNER_BIN_NAME))
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
        let (linera_sdk_dep, linera_sdk_dev_dep) = Self::linera_sdk_dependencies();
        let toml_contents = format!(
            include_str!("../template/Cargo.toml"),
            project_name = project_name,
            linera_sdk_dep = linera_sdk_dep,
            linera_sdk_dev_dep = linera_sdk_dev_dep
        );
        Self::write_string_to_file(&toml_path, &toml_contents)
    }

    fn create_state_file(source_directory: &Path) -> Result<()> {
        let state_path = source_directory.join("state.rs");
        Self::write_string_to_file(&state_path, include_str!("../template/state.rs.template"))
    }

    fn create_lib_file(source_directory: &Path) -> Result<()> {
        let state_path = source_directory.join("lib.rs");
        Self::write_string_to_file(&state_path, include_str!("../template/lib.rs.template"))
    }

    fn create_contract_file(source_directory: &Path, project_name: &str) -> Result<()> {
        let project_name = project_name.replace('-', "_");
        let contract_path = source_directory.join("contract.rs");
        let contract_contents = format!(
            include_str!("../template/contract.rs.template"),
            project_name = project_name
        );
        Self::write_string_to_file(&contract_path, &contract_contents)
    }

    fn create_service_file(source_directory: &Path, project_name: &str) -> Result<()> {
        let project_name = project_name.replace('-', "_");
        let service_path = source_directory.join("service.rs");
        let service_contents = format!(
            include_str!("../template/service.rs.template"),
            project_name = project_name
        );
        Self::write_string_to_file(&service_path, &service_contents)
    }

    fn create_cargo_config(project_root: &Path) -> Result<()> {
        let config_dir_path = project_root.join(".cargo");
        let config_file_path = config_dir_path.join("config.toml");
        std::fs::create_dir(&config_dir_path)?;
        Self::write_string_to_file(&config_file_path, include_str!("../template/config.toml"))
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
    fn linera_sdk_dependencies() -> (String, String) {
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
        let linera_sdk_dev_dep = format!(
            "linera-sdk = {{ path = \"{}\", features = [\"test\"] }}",
            linera_sdk_path.display()
        );
        (linera_sdk_dep, linera_sdk_dev_dep)
    }

    /// Adds ['linera-sdk'] dependencies in `release` mode.
    ///
    /// Includes the contents of `linera-sdk`'s `Cargo.toml` at compile time
    /// to figure out the latest version.
    #[cfg(not(debug_assertions))]
    fn linera_sdk_dependencies() -> (String, String) {
        let content = include_str!("../../linera-sdk/Cargo.toml");
        let sdk_cargo_toml: toml::Value = toml::from_str(content)
            .expect("there was an error parsing a TOML file included at compile-time - this should never happen.");
        let version = sdk_cargo_toml["package"]["version"].as_str()
            .expect("there was an error finding the version in a TOML file included at compile-time - this should never happen.");
        let linera_sdk_dep = format!("linera-sdk = \"{}\"", version);
        let linera_sdk_dev_dep = format!(
            "linera-sdk = {{ version = \"{}\", features = [\"test\"] }}",
            version
        );
        (linera_sdk_dep, linera_sdk_dev_dep)
    }
}
