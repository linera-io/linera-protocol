// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generator of WIT files representing the interface between Linera applications and nodes.

#![cfg_attr(target_arch = "wasm32", no_main)]
#![cfg(not(target_arch = "wasm32"))]

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use clap::Parser as _;
use linera_execution::{
    ContractEntrypoints, ContractSyncRuntimeHandle, ContractSystemApi, ServiceEntrypoints,
    ServiceSyncRuntimeHandle, ServiceSystemApi, SystemApiData, ViewSystemApi,
};
use linera_witty::wit_generation::{StubInstance, WitInterfaceWriter, WitWorldWriter};

/// Command line parameters for the WIT generator.
#[derive(Debug, clap::Parser)]
pub struct WitGeneratorOptions {
    /// The base directory of where the WIT files should be placed.
    #[arg(short, long, default_value = "linera-sdk/wit")]
    base_directory: PathBuf,

    /// Check if the existing files are correct.
    #[arg(short, long)]
    check: bool,
}

/// WIT file generator entrypoint.
fn main() -> Result<()> {
    let options = WitGeneratorOptions::parse();

    if options.check {
        run_operation(options, CheckFile)?;
    } else {
        run_operation(options, WriteToFile)?;
    }

    Ok(())
}

/// Runs the main `operation` on all the WIT files.
fn run_operation(options: WitGeneratorOptions, mut operation: impl Operation) -> Result<()> {
    let contract_entrypoints = WitInterfaceWriter::new::<ContractEntrypoints<StubInstance>>();
    let service_entrypoints = WitInterfaceWriter::new::<ServiceEntrypoints<StubInstance>>();

    let contract_system_api = WitInterfaceWriter::new::<
        ContractSystemApi<StubInstance<SystemApiData<ContractSyncRuntimeHandle>>>,
    >();
    let service_system_api = WitInterfaceWriter::new::<
        ServiceSystemApi<StubInstance<SystemApiData<ServiceSyncRuntimeHandle>>>,
    >();
    let view_system_api = WitInterfaceWriter::new::<
        ViewSystemApi<StubInstance<SystemApiData<ContractSyncRuntimeHandle>>>,
    >();

    let contract_world = WitWorldWriter::new("linera:app", "contract")
        .export::<ContractEntrypoints<StubInstance>>()
        .import::<ContractSystemApi<StubInstance<SystemApiData<ContractSyncRuntimeHandle>>>>()
        .import::<ViewSystemApi<StubInstance<SystemApiData<ContractSyncRuntimeHandle>>>>();
    let service_world = WitWorldWriter::new("linera:app", "service")
        .export::<ServiceEntrypoints<StubInstance>>()
        .import::<ServiceSystemApi<StubInstance<SystemApiData<ServiceSyncRuntimeHandle>>>>()
        .import::<ViewSystemApi<StubInstance<SystemApiData<ContractSyncRuntimeHandle>>>>();

    operation.run_for_file(
        &options.base_directory.join("contract-entrypoints.wit"),
        contract_entrypoints.generate_file_contents(),
    )?;
    operation.run_for_file(
        &options.base_directory.join("service-entrypoints.wit"),
        service_entrypoints.generate_file_contents(),
    )?;

    operation.run_for_file(
        &options.base_directory.join("contract-system-api.wit"),
        contract_system_api.generate_file_contents(),
    )?;
    operation.run_for_file(
        &options.base_directory.join("service-system-api.wit"),
        service_system_api.generate_file_contents(),
    )?;
    operation.run_for_file(
        &options.base_directory.join("view-system-api.wit"),
        view_system_api.generate_file_contents(),
    )?;

    operation.run_for_file(
        &options.base_directory.join("contract.wit"),
        contract_world.generate_file_contents(),
    )?;
    operation.run_for_file(
        &options.base_directory.join("service.wit"),
        service_world.generate_file_contents(),
    )?;

    Ok(())
}

/// An operation that this WIT generator binary can perform.
trait Operation {
    /// Executes the operation for a file at `path`, using the WIT `contents`.
    fn run_for_file<'c>(
        &mut self,
        path: &Path,
        contents: impl Iterator<Item = &'c str>,
    ) -> Result<()>;
}

/// Writes out the WIT file.
pub struct WriteToFile;

impl Operation for WriteToFile {
    fn run_for_file<'c>(
        &mut self,
        path: &Path,
        contents: impl Iterator<Item = &'c str>,
    ) -> Result<()> {
        let mut file = BufWriter::new(
            File::create(path)
                .with_context(|| format!("Failed to create file at {}", path.display()))?,
        );

        for part in contents {
            file.write_all(part.as_bytes())
                .with_context(|| format!("Failed to write to {}", path.display()))?;
        }

        file.flush()
            .with_context(|| format!("Failed to flush to {}", path.display()))?;

        Ok(())
    }
}

/// Checks that a WIT file has the expected contents.
pub struct CheckFile;

impl Operation for CheckFile {
    fn run_for_file<'c>(
        &mut self,
        path: &Path,
        contents: impl Iterator<Item = &'c str>,
    ) -> Result<()> {
        let mut buffer = Vec::new();
        let mut file = BufReader::new(
            File::open(path)
                .with_context(|| format!("Failed to open file at {}", path.display()))?,
        );

        for part in contents {
            buffer.resize(part.as_bytes().len(), 0);
            file.read_exact(&mut buffer)
                .with_context(|| format!("Failed to read from {}", path.display()))?;

            ensure!(
                buffer == part.as_bytes(),
                format!("WIT file {} does not match", path.display())
            );
        }

        buffer.resize(1, 0);
        ensure!(
            file.read(&mut buffer)? == 0,
            format!("WIT file {} has extra contents", path.display())
        );

        Ok(())
    }
}
