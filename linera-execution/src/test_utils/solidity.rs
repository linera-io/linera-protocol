use std::{
    fs::File,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

use anyhow::Context;
use tempfile::tempdir;

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
        .context("failed to get contracts")?;
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

pub fn get_example_counter() -> anyhow::Result<Vec<u8>> {
    let source_code = r#"
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract ExampleCounter {
  uint256 value;
  constructor(uint256 start_value) {
    value = start_value;
  }

  function increment(uint256 input) external returns (uint256) {
    value = value + input;
    return value;
  }

  function get_value() external view returns (uint256) {
    return value;
  }

}
"#
    .to_string();
    get_bytecode(&source_code, "ExampleCounter")
}
