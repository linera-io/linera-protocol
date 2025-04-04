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
use serde_json::Value;
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





pub fn get_evm_call_wasm_example_counter() -> anyhow::Result<Vec<u8>> {
    let source_code = r#"
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract ExampleCallWasmCounter {
  bytes32 universal_address;
  constructor(bytes32 _universal_address) {
    universal_address = _universal_address;
  }


  struct CounterOperation {
    uint8 choice;
    // choice=0 corresponds to Increment
    uint64 increment;
  }
  function bcs_serialize_CounterOperation(CounterOperation memory input) internal pure returns (bytes memory) {
    bytes memory result = abi.encodePacked(input.choice);
    if (input.choice == 0) {
      return abi.encodePacked(result, bcs_serialize_uint64(input.increment));
    }
    return result;
  }


  struct CounterRequest {
    uint8 choice;
    // choice=0 corresponds to Query
    // choice=1 corresponds to Increment
    uint64 increment;
  }




  function bcs_serialize_uint64(uint64 input) internal pure returns (bytes memory) {
    bytes memory result = new bytes(8);
    uint64 value = input;
    result[0] = bytes1(uint8(value));
    for (uint i=1; i<8; i++) {
      value = value >> 8;
      result[i] = bytes1(uint8(value));
    }
    return result;
  }
  function bcs_deserialize_offset_uint64(uint256 pos, bytes memory input) internal pure returns (uint256, uint64) {
    require(pos + 7 < input.length, "Position out of bound");
    uint64 value = uint8(input[pos + 7]);
    for (uint256 i=0; i<7; i++) {
      value = value << 8;
      value += uint8(input[pos + 6 - i]);
    }
    return (pos + 8, value);
  }

  function bcs_deserialize_offset_uint8(uint256 pos, bytes memory input) internal pure returns (uint256, uint8) {
    require(pos < input.length, "Position out of bound");
    uint8 value = uint8(input[pos]);
    return (pos + 1, value);
  }



  function bcs_deserialize_uint64(bytes memory input) public pure returns (uint64) {
    uint256 new_pos;
    uint64 value;
    (new_pos, value) = bcs_deserialize_offset_uint64(0, input);
    require(new_pos == input.length, "incomplete deserialization");
    return value;
  }

  function serde_json_serialize_uint64(uint64 input) public pure returns (bytes memory) {
    if (input < 10) {
      bytes memory data = abi.encodePacked(uint8(48 + input));
      return data;
    }
    if (input < 100) {
      uint64 val1 = input % 10;
      uint64 val2 = (input - val1) / 10;
      bytes memory data = abi.encodePacked(uint8(48 + val2), uint8(48 + val1));
      return data;
    }
    require(false);
  }


  function serde_json_serialize_CounterRequest(CounterRequest memory input) public pure returns (bytes memory) {
    if (input.choice == 0) {
      bytes memory data = abi.encodePacked(uint8(34), uint8(81), uint8(117), uint8(101), uint8(114), uint8(121), uint8(34));
      return data;
    }
    bytes memory blk1 = abi.encodePacked(uint8(123), uint8(34), uint8(73), uint8(110), uint8(99), uint8(114), uint8(101), uint8(109), uint8(101), uint8(110), uint8(116), uint8(34), uint8(58));
    bytes memory blk2 = serde_json_serialize_uint64(input.increment);
    bytes memory blk3 = abi.encodePacked(uint8(125));
    return abi.encodePacked(blk1, blk2, blk3);
  }

  function serde_json_deserialize_u64(bytes memory input) public pure returns (uint64) {
    uint64 value = 0;
    uint256 len = input.length;
    uint64 pow = 1;
    for (uint256 idx=0; idx<len; idx++) {
      uint256 jdx = len - 1 - idx;
      uint8 val_idx = uint8(input[jdx]) - 48;
      value = value + uint64(val_idx) * pow;
      pow = pow * 10;
    }
    return value;
  }


  function nest_increment(uint64 input1) external returns (uint64) {
    address precompile = address(0x0b);
    CounterOperation memory input2 = CounterOperation({choice: 0, increment: input1});
    bytes memory input3 = bcs_serialize_CounterOperation(input2);
    bytes memory input4 = abi.encodePacked(universal_address, input3);
    (bool success, bytes memory return1) = precompile.call(input4);
    require(success);
    uint64 return2 = bcs_deserialize_uint64(return1);
    return return2;
  }

  function nest_get_value() external returns (uint64) {
    address precompile = address(0x0b);
    CounterRequest memory input2 = CounterRequest({choice:0, increment: 0});
    bytes memory input3 = serde_json_serialize_CounterRequest(input2);
    bytes memory input4 = abi.encodePacked(universal_address, input3);
    (bool success, bytes memory return1) = precompile.call(input4);
    require(success);
    uint64 return2 = serde_json_deserialize_u64(return1);
    return return2;
  }

}
"#
    .to_string();
    get_bytecode(&source_code, "ExampleCallWasmCounter")
}




pub fn get_evm_call_evm_example_counter() -> anyhow::Result<Vec<u8>> {
    let source_code = r#"
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

interface IExternalContract {
  function increment(uint64 value) external returns (uint64);
  function get_value() external returns (uint64);
}


contract ExampleCallEvmCounter {
  address evm_address;
  constructor(address _evm_address) {
    evm_address = _evm_address;
  }
  function nest_increment(uint64 input) external returns (uint64) {
    IExternalContract externalContract = IExternalContract(evm_address);
    return externalContract.increment(input);
  }
  function nest_get_value() external returns (uint64) {
    IExternalContract externalContract = IExternalContract(evm_address);
    return externalContract.get_value();
  }
}
"#
    .to_string();
    get_bytecode(&source_code, "ExampleCallEvmCounter")
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
