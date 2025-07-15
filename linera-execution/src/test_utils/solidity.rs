// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code for compiling solidity smart contracts for testing purposes.

use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::Context;
use revm_primitives::U256;
use serde_json::Value;
use tempfile::{tempdir, TempDir};

use crate::{LINERA_SOL, LINERA_TYPES_SOL};

fn write_compilation_json(path: &Path, file_name: &str) -> anyhow::Result<()> {
    let mut source = File::create(path).unwrap();
    writeln!(
        source,
        r#"
{{
  "language": "Solidity",
  "sources": {{
    "{file_name}": {{
      "urls": ["./{file_name}"]
    }}
  }},
  "settings": {{
    "viaIR": true,
    "outputSelection": {{
      "*": {{
        "*": ["evm.bytecode"]
      }}
    }}
  }}
}}
"#
    )?;
    Ok(())
}

fn get_bytecode_path(path: &Path, file_name: &str, contract_name: &str) -> anyhow::Result<Vec<u8>> {
    let config_path = path.join("config.json");
    write_compilation_json(&config_path, file_name)?;
    let config_file = File::open(config_path)?;

    let output_path = path.join("result.json");
    let output_file = File::create(output_path.clone())?;

    let status = Command::new("solc")
        .current_dir(path)
        .arg("--standard-json")
        .stdin(Stdio::from(config_file))
        .stdout(Stdio::from(output_file))
        .status()?;
    assert!(status.success());

    let contents = std::fs::read_to_string(output_path)?;
    let json_data: serde_json::Value = serde_json::from_str(&contents)?;
    let contracts = json_data
        .get("contracts")
        .with_context(|| format!("failed to get contracts in json_data={json_data}"))?;
    let file_name_contract = contracts
        .get(file_name)
        .context("failed to get {file_name}")?;
    let test_data = file_name_contract
        .get(contract_name)
        .with_context(|| format!("failed to get contract_name={contract_name}"))?;
    let evm_data = test_data
        .get("evm")
        .with_context(|| format!("failed to get evm in test_data={test_data}"))?;
    let bytecode = evm_data
        .get("bytecode")
        .with_context(|| format!("failed to get bytecode in evm_data={evm_data}"))?;
    let object = bytecode
        .get("object")
        .with_context(|| format!("failed to get object in bytecode={bytecode}"))?;
    let object = object.to_string();
    let object = object.trim_matches(|c| c == '"').to_string();
    Ok(hex::decode(&object)?)
}

pub fn get_bytecode(source_code: &str, contract_name: &str) -> anyhow::Result<Vec<u8>> {
    let dir = tempdir().unwrap();
    let path = dir.path();
    if source_code.contains("Linera.sol") {
        // The source code seems to import Linera.sol, so we import the relevant files.
        for (file_name, literal_path) in [
            ("Linera.sol", LINERA_SOL),
            ("LineraTypes.sol", LINERA_TYPES_SOL),
        ] {
            let test_code_path = path.join(file_name);
            let mut test_code_file = File::create(&test_code_path)?;
            writeln!(test_code_file, "{}", literal_path)?;
        }
    }
    if source_code.contains("@openzeppelin") {
        let _output = Command::new("npm")
            .args(["install", "@openzeppelin/contracts"])
            .current_dir(path)
            .output()?;
        let _output = Command::new("mv")
            .args(["node_modules/@openzeppelin", "@openzeppelin"])
            .current_dir(path)
            .output()?;
    }
    let file_name = "test_code.sol";
    let test_code_path = path.join(file_name);
    let mut test_code_file = File::create(&test_code_path)?;
    writeln!(test_code_file, "{}", source_code)?;
    get_bytecode_path(path, file_name, contract_name)
}

pub fn load_solidity_example(path: &str) -> anyhow::Result<Vec<u8>> {
    let source_code = std::fs::read_to_string(path)?;
    let contract_name: &str = source_code
        .lines()
        .filter_map(|line| line.trim_start().strip_prefix("contract "))
        .next()
        .ok_or_else(|| anyhow::anyhow!("Not matching"))?;
    let contract_name: &str = contract_name
        .split_whitespace()
        .next()
        .ok_or(anyhow::anyhow!("No space found after the contract name"))?;
    get_bytecode(&source_code, contract_name)
}

pub fn temporary_write_evm_module(module: Vec<u8>) -> anyhow::Result<(PathBuf, TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path();
    let app_file = "app.json";
    let app_path = path.join(app_file);
    {
        std::fs::write(app_path.clone(), &module)?;
    }
    let evm_contract = app_path.to_path_buf();
    Ok((evm_contract, dir))
}

pub fn get_evm_contract_path(path: &str) -> anyhow::Result<(PathBuf, TempDir)> {
    let module = load_solidity_example(path)?;
    temporary_write_evm_module(module)
}

pub fn value_to_vec_u8(value: Value) -> Vec<u8> {
    let mut vec: Vec<u8> = Vec::new();
    for val in value.as_array().unwrap() {
        let val = val.as_u64().unwrap();
        let val = val as u8;
        vec.push(val);
    }
    vec
}

pub fn read_evm_u64_entry(value: Value) -> u64 {
    let vec = value_to_vec_u8(value);
    let mut arr = [0_u8; 8];
    arr.copy_from_slice(&vec[24..]);
    u64::from_be_bytes(arr)
}

pub fn read_evm_u256_entry(value: Value) -> U256 {
    let result = value
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().ok_or("Not a number").map(|n| n as u8))
        .collect::<Result<Vec<u8>, _>>()
        .unwrap();
    U256::from_be_slice(&result)
}
