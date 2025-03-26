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
use tempfile::{tempdir, TempDir};

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
        .with_context(|| format!("failed to get contracts in json_data={}", json_data))?;
    let file_name_contract = contracts
        .get(file_name)
        .context("failed to get {file_name}")?;
    let test_data = file_name_contract
        .get(contract_name)
        .context("failed to get contract_name={contract_name}")?;
    let evm_data = test_data.get("evm").context("failed to get evm")?;
    let bytecode = evm_data.get("bytecode").context("failed to get bytecode")?;
    let object = bytecode.get("object").context("failed to get object")?;
    let object = object.to_string();
    let object = object.trim_matches(|c| c == '"').to_string();
    Ok(hex::decode(&object)?)
}

pub fn get_bytecode(source_code: &str, contract_name: &str) -> anyhow::Result<Vec<u8>> {
    let dir = tempdir().unwrap();
    let path = dir.path();
    let file_name = "test_code.sol";
    let test_code_path = path.join(file_name);
    let mut test_code_file = File::create(&test_code_path)?;
    writeln!(test_code_file, "{}", source_code)?;
    get_bytecode_path(path, file_name, contract_name)
}

pub fn get_evm_example_counter() -> anyhow::Result<Vec<u8>> {
    let source_code = r#"
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract ExampleCounter {
  uint64 value;
  constructor(uint64 start_value) {
    value = start_value;
  }

  function increment(uint64 input) external returns (uint64) {
    value = value + input;
    return value;
  }

  function get_value() external view returns (uint64) {
    return value;
  }

}
"#
    .to_string();
    get_bytecode(&source_code, "ExampleCounter")
}

pub fn get_contract_service_paths(module: Vec<u8>) -> anyhow::Result<(PathBuf, PathBuf, TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path();
    let app_file = "app.json";
    let app_path = path.join(app_file);
    {
        std::fs::write(app_path.clone(), &module)?;
    }
    let evm_contract = app_path.to_path_buf();
    let evm_service = app_path.to_path_buf();
    Ok((evm_contract, evm_service, dir))
}

fn value_to_vec_u8(value: Value) -> Vec<u8> {
    let mut vec: Vec<u8> = Vec::new();
    for val in value.as_array().unwrap() {
        let val = val.as_u64().unwrap();
        let val = val as u8;
        vec.push(val);
    }
    vec
}

fn read_evm_u64_entry(value: Value) -> anyhow::Result<u64> {
    let vec = value_to_vec_u8(value);
    let mut arr = [0_u8; 8];
    arr.copy_from_slice(&vec[24..]);
    let counter_value = u64::from_be_bytes(arr);
    Ok(counter_value)
}
