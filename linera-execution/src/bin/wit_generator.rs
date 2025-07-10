// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generator of WIT files representing the interface between Linera applications and nodes.

#![cfg_attr(any(target_arch = "wasm32", not(with_wasm_runtime)), no_main)]
#![cfg(all(not(target_arch = "wasm32"), with_wasm_runtime))]

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::Parser as _;
use linera_execution::{
    BaseRuntimeApi, ContractEntrypoints, ContractRuntimeApi, ContractSyncRuntimeHandle,
    RuntimeApiData, ServiceEntrypoints, ServiceRuntimeApi, ServiceSyncRuntimeHandle,
};
use linera_witty::wit_generation::{
    FileContentGenerator, StubInstance, WitInterfaceWriter, WitWorldWriter,
};

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

    let base_runtime_api = WitInterfaceWriter::new::<
        BaseRuntimeApi<StubInstance<RuntimeApiData<ContractSyncRuntimeHandle>>>,
    >();
    let contract_runtime_api = WitInterfaceWriter::new::<
        ContractRuntimeApi<StubInstance<RuntimeApiData<ContractSyncRuntimeHandle>>>,
    >();
    let service_runtime_api = WitInterfaceWriter::new::<
        ServiceRuntimeApi<StubInstance<RuntimeApiData<ServiceSyncRuntimeHandle>>>,
    >();

    let contract_world = WitWorldWriter::new("linera:app", "contract")
        .export::<ContractEntrypoints<StubInstance>>()
        .import::<ContractRuntimeApi<StubInstance<RuntimeApiData<ContractSyncRuntimeHandle>>>>()
        .import::<BaseRuntimeApi<StubInstance<RuntimeApiData<ContractSyncRuntimeHandle>>>>();
    let service_world = WitWorldWriter::new("linera:app", "service")
        .export::<ServiceEntrypoints<StubInstance>>()
        .import::<ServiceRuntimeApi<StubInstance<RuntimeApiData<ServiceSyncRuntimeHandle>>>>()
        .import::<BaseRuntimeApi<StubInstance<RuntimeApiData<ContractSyncRuntimeHandle>>>>();

    operation.run_for_file(
        &options.base_directory.join("contract-entrypoints.wit"),
        contract_entrypoints,
    )?;
    operation.run_for_file(
        &options.base_directory.join("service-entrypoints.wit"),
        service_entrypoints,
    )?;

    operation.run_for_file(
        &options.base_directory.join("base-runtime-api.wit"),
        base_runtime_api,
    )?;
    operation.run_for_file(
        &options.base_directory.join("contract-runtime-api.wit"),
        contract_runtime_api,
    )?;
    operation.run_for_file(
        &options.base_directory.join("service-runtime-api.wit"),
        service_runtime_api,
    )?;

    operation.run_for_file(&options.base_directory.join("contract.wit"), contract_world)?;
    operation.run_for_file(&options.base_directory.join("service.wit"), service_world)?;

    Ok(())
}

/// An operation that this WIT generator binary can perform.
trait Operation {
    /// Executes the operation for a file at `path`, using the WIT `contents`.
    fn run_for_file(&mut self, path: &Path, generator: impl FileContentGenerator) -> Result<()>;
}

/// Writes out the WIT file.
pub struct WriteToFile;

impl Operation for WriteToFile {
    fn run_for_file(&mut self, path: &Path, generator: impl FileContentGenerator) -> Result<()> {
        let mut file = BufWriter::new(
            File::create(path)
                .with_context(|| format!("Failed to create file at {}", path.display()))?,
        );
        generator
            .generate_file_contents(&mut file)
            .with_context(|| format!("Failed to write to {}", path.display()))?;

        file.flush()
            .with_context(|| format!("Failed to flush to {}", path.display()))?;

        Ok(())
    }
}

/// Checks that a WIT file has the expected contents.
pub struct CheckFile;

struct FileComparator {
    buffer: Vec<u8>,
    file: BufReader<File>,
}

impl FileComparator {
    fn new(file: File) -> Self {
        Self {
            buffer: Vec::new(),
            file: BufReader::new(file),
        }
    }
}

impl Write for FileComparator {
    fn write(&mut self, part: &[u8]) -> std::io::Result<usize> {
        self.buffer.resize(part.len(), 0);
        self.file.read_exact(&mut self.buffer)?;
        if self.buffer != part {
            return Err(std::io::Error::other("file does not match"));
        }
        Ok(part.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer.resize(1, 0);
        if self.file.read(&mut self.buffer)? != 0 {
            return Err(std::io::Error::other("file has extra contents"));
        }
        Ok(())
    }
}

impl Operation for CheckFile {
    fn run_for_file(&mut self, path: &Path, generator: impl FileContentGenerator) -> Result<()> {
        let mut file_comparer = FileComparator::new(
            File::open(path)
                .with_context(|| format!("Failed to open file at {}", path.display()))?,
        );
        generator
            .generate_file_contents(&mut file_comparer)
            .with_context(|| format!("Comparison with {} failed", path.display()))?;
        file_comparer
            .flush()
            .with_context(|| format!("Comparison with {} failed", path.display()))?;

        Ok(())
    }
}
