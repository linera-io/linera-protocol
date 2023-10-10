// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::util;
use anyhow::{anyhow, bail, Context, Result};
use cargo_toml::Manifest;
use current_platform::CURRENT_PLATFORM;
use std::{
    ffi::OsStr,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};
use tracing::debug;

pub struct Project {
    root: PathBuf,
}

const RUNNER_BIN_NAME: &str = "linera-wasm-test-runner";
const RUNNER_BIN_CRATE: &str = "linera-sdk";

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

        debug!("initializing git repository");
        Self::initialize_git_repository(&root)?;

        let project_name = root
            .file_name()
            .and_then(OsStr::to_str)
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("path specified cannot terminate in . or .."))?;

        debug!("writing Cargo.toml");
        Self::create_cargo_toml(&root, &project_name)?;

        debug!("writing rust-toolchain");
        Self::create_rust_toolchain(&root)?;

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

    pub async fn test(&self) -> Result<()> {
        let runner_path = util::resolve_binary(RUNNER_BIN_NAME, RUNNER_BIN_CRATE).await?;
        let unit_tests = Command::new("cargo")
            .env(
                "CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER",
                runner_path.display().to_string().as_str(),
            )
            .arg("test")
            .args(["--target", "wasm32-unknown-unknown"])
            .current_dir(&self.root)
            .spawn()?
            .wait()?;
        if !unit_tests.success() {
            bail!("unit tests failed")
        }
        let integration_tests = Command::new("cargo")
            .arg("test")
            .args(["--target", CURRENT_PLATFORM])
            .current_dir(&self.root)
            .spawn()?
            .wait()?;
        if !integration_tests.success() {
            bail!("integration tests failed")
        }
        Ok(())
    }

    /// Finds the workspace for a given crate. If the workspace
    /// does not exist, returns the path of the crate.
    fn workspace_root(&self) -> Result<&Path> {
        let mut current_path = self.root.as_path();
        loop {
            let toml_path = current_path.join("Cargo.toml");
            if toml_path.exists() {
                let toml = Manifest::from_path(toml_path)?;
                if toml.workspace.is_some() {
                    return Ok(current_path);
                }
            }
            match current_path.parent() {
                None => {
                    break;
                }
                Some(parent) => current_path = parent,
            }
        }
        Ok(self.root.as_path())
    }

    fn create_source_directory(project_root: &Path) -> Result<PathBuf> {
        let source_directory = project_root.join("src");
        std::fs::create_dir(&source_directory)?;
        Ok(source_directory)
    }

    fn initialize_git_repository(project_root: &Path) -> Result<()> {
        let output = Command::new("git")
            .args([
                "init",
                project_root
                    .to_str()
                    .context("project name contains non UTF-8 characters")?,
            ])
            .output()?;

        if !output.status.success() {
            bail!(
                "failed to initialize git repository at {}",
                &project_root.display()
            );
        }

        Self::write_string_to_file(&project_root.join(".gitignore"), "/target")
    }

    fn create_cargo_toml(project_root: &Path, project_name: &str) -> Result<()> {
        let toml_path = project_root.join("Cargo.toml");
        let (linera_sdk_dep, linera_sdk_dev_dep, linera_views_dep) =
            Self::linera_sdk_dependencies();
        let toml_contents = format!(
            include_str!("../template/Cargo.toml.template"),
            project_name = project_name,
            linera_sdk_dep = linera_sdk_dep,
            linera_sdk_dev_dep = linera_sdk_dev_dep,
            linera_views_dep = linera_views_dep
        );
        Self::write_string_to_file(&toml_path, &toml_contents)
    }

    fn create_rust_toolchain(project_root: &Path) -> Result<()> {
        let toolchain_path = project_root.join("rust-toolchain");
        Self::write_string_to_file(
            &toolchain_path,
            include_str!("../template/rust-toolchain.template"),
        )
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
        Self::write_string_to_file(
            &config_file_path,
            include_str!("../template/config.toml.template"),
        )
    }

    fn write_string_to_file(path: &Path, content: &str) -> Result<()> {
        let mut file = File::create(path)?;
        file.write_all(content.as_bytes())?;
        Ok(())
    }

    /// Resolves ['linera-sdk'] and [`linera-views`] dependencies in `debug` mode.
    ///
    /// Uses the directory of `linera-service` at compile time to figure out
    /// where `linera-sdk` and `linera-views' is.
    #[cfg(debug_assertions)]
    fn linera_sdk_dependencies() -> (String, String, String) {
        let linera_service_path: PathBuf = env!("CARGO_MANIFEST_DIR")
            .parse()
            .expect("the CARGO_MANIFEST_DIR should always be a valid path");
        let linera_sdk_path = linera_service_path
            .join("..")
            .join("linera-sdk")
            .canonicalize()
            .expect("the linera-sdk crate should always exist");
        let linera_views_path = linera_service_path
            .join("..")
            .join("linera-views")
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
        let linera_views_dep = format!(
            "linera-views = {{ path = \"{}\" }}",
            linera_views_path.display()
        );
        (linera_sdk_dep, linera_sdk_dev_dep, linera_views_dep)
    }

    /// Adds ['linera-sdk'] dependencies in `release` mode.
    #[cfg(not(debug_assertions))]
    fn linera_sdk_dependencies() -> (String, String, String) {
        let version = env!("CARGO_PKG_VERSION");
        let linera_sdk_dep = format!("linera-sdk = \"{}\"", version);
        let linera_sdk_dev_dep = format!(
            "linera-sdk = {{ version = \"{}\", features = [\"test\"] }}",
            version
        );
        let linera_views_dep = format!("linera-views = \"{}\"", version);
        (linera_sdk_dep, linera_sdk_dev_dep, linera_views_dep)
    }

    pub fn build(&self, name: Option<String>) -> Result<(PathBuf, PathBuf), anyhow::Error> {
        let name = name.unwrap_or(self.project_package_name()?);
        let contract_name = format!("{}_contract", name);
        let service_name = format!("{}_service", name);
        let cargo_build = Command::new("cargo")
            .arg("build")
            .arg("--release")
            .args(["--target", "wasm32-unknown-unknown"])
            .current_dir(&self.root)
            .spawn()?
            .wait()?;
        if !cargo_build.success() {
            bail!("build failed")
        }
        let build_path = self
            .workspace_root()?
            .join("target/wasm32-unknown-unknown/release");
        Ok((
            build_path.join(contract_name).with_extension("wasm"),
            build_path.join(service_name).with_extension("wasm"),
        ))
    }

    fn project_package_name(&self) -> Result<String> {
        let manifest = Manifest::from_path(self.cargo_toml_path())?;
        let name = manifest
            .package
            .context("Cargo.toml is missing `[package]`")?
            .name;
        Ok(name)
    }

    fn cargo_toml_path(&self) -> PathBuf {
        self.root.join("Cargo.toml")
    }
}
