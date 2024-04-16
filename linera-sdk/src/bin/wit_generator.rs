// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generator of WIT files representing the interface between Linera applications and nodes.

use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

use clap::Parser as _;
use linera_execution::{
    ContractEntrypoints, ContractSyncRuntime, ContractSystemApi, ServiceEntrypoints,
    ServiceSyncRuntime, ServiceSystemApi, SystemApiData, ViewSystemApi,
};
use linera_sdk::MockSystemApi;
use linera_witty::{
    wit_generation::{WitInterfaceWriter, WitWorldWriter},
    MockInstance,
};

/// Command line parameters for the WIT generator.
#[derive(Debug, clap::Parser)]
pub struct WitGeneratorOptions {
    /// The base directory of where the WIT files should be placed.
    #[arg(short, long, default_value = "linera-sdk/wit")]
    base_directory: PathBuf,
}

/// WIT file generator entrypoint.
fn main() -> Result<(), io::Error> {
    let options = WitGeneratorOptions::parse();

    let contract_entrypoints = WitInterfaceWriter::new::<ContractEntrypoints<MockInstance<()>>>();
    let service_entrypoints = WitInterfaceWriter::new::<ServiceEntrypoints<MockInstance<()>>>();
    let mock_system_api = WitInterfaceWriter::new::<MockSystemApi<MockInstance<()>>>();

    let contract_system_api = WitInterfaceWriter::new::<
        ContractSystemApi<MockInstance<SystemApiData<ContractSyncRuntime>>>,
    >();
    let service_system_api = WitInterfaceWriter::new::<
        ServiceSystemApi<MockInstance<SystemApiData<ServiceSyncRuntime>>>,
    >();
    let view_system_api = WitInterfaceWriter::new::<
        ViewSystemApi<MockInstance<SystemApiData<ContractSyncRuntime>>>,
    >();

    let contract_world = WitWorldWriter::new("linera:app", "contract")
        .export::<ContractEntrypoints<MockInstance<()>>>()
        .import::<ContractSystemApi<MockInstance<SystemApiData<ContractSyncRuntime>>>>()
        .import::<ViewSystemApi<MockInstance<SystemApiData<ContractSyncRuntime>>>>();
    let service_world = WitWorldWriter::new("linera:app", "service")
        .export::<ServiceEntrypoints<MockInstance<()>>>()
        .import::<ServiceSystemApi<MockInstance<SystemApiData<ServiceSyncRuntime>>>>()
        .import::<ViewSystemApi<MockInstance<SystemApiData<ContractSyncRuntime>>>>();
    let unit_tests_world = WitWorldWriter::new("linera:app", "unit-tests")
        .export::<MockSystemApi<MockInstance<()>>>()
        .import::<ContractSystemApi<MockInstance<SystemApiData<ContractSyncRuntime>>>>()
        .import::<ServiceSystemApi<MockInstance<SystemApiData<ServiceSyncRuntime>>>>()
        .import::<ViewSystemApi<MockInstance<SystemApiData<ContractSyncRuntime>>>>();

    write_to_file(
        &options.base_directory.join("contract-entrypoints.wit"),
        contract_entrypoints.generate_file_contents(),
    )?;
    write_to_file(
        &options.base_directory.join("service-entrypoints.wit"),
        service_entrypoints.generate_file_contents(),
    )?;
    write_to_file(
        &options.base_directory.join("mock-system-api.wit"),
        mock_system_api.generate_file_contents(),
    )?;

    write_to_file(
        &options.base_directory.join("contract-system-api.wit"),
        contract_system_api.generate_file_contents(),
    )?;
    write_to_file(
        &options.base_directory.join("service-system-api.wit"),
        service_system_api.generate_file_contents(),
    )?;
    write_to_file(
        &options.base_directory.join("view-system-api.wit"),
        view_system_api.generate_file_contents(),
    )?;

    write_to_file(
        &options.base_directory.join("contract.wit"),
        contract_world.generate_file_contents(),
    )?;
    write_to_file(
        &options.base_directory.join("service.wit"),
        service_world.generate_file_contents(),
    )?;
    write_to_file(
        &options.base_directory.join("unit-tests.wit"),
        unit_tests_world.generate_file_contents(),
    )?;

    Ok(())
}

/// Writes the provided `contents` to a new file at the specified `path`.
fn write_to_file<'c>(path: &Path, contents: impl Iterator<Item = &'c str>) -> io::Result<()> {
    let mut file = BufWriter::new(File::create(path)?);
    for part in contents {
        file.write_all(part.as_bytes())?;
    }
    file.flush()
}
