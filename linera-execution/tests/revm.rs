// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(with_revm)]

use std::{
    fs::File,
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
};

use alloy_sol_types::{sol, SolCall, SolValue};
use anyhow::Context;
use linera_base::{
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{ChainDescription, ChainId},
};
use linera_execution::{
    revm::{EvmContractModule, EvmServiceModule},
    test_utils::{create_dummy_user_application_description, SystemExecutionState},
    ExecutionRuntimeConfig, ExecutionRuntimeContext, Operation, OperationContext, Query,
    QueryContext, QueryResponse, ResourceControlPolicy, ResourceController, ResourceTracker,
    TransactionTracker,
};
use linera_views::{context::Context as _, views::View};
use revm_primitives::U256;
use tempfile::tempdir;

fn write_compilation_json(path: &Path, file_name: &str) {
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
    )
    .unwrap();
}

fn get_bytecode_path(path: &Path, file_name: &str, contract_name: &str) -> anyhow::Result<Vec<u8>> {
    let config_path = path.join("config.json");
    write_compilation_json(&config_path, file_name);
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

fn get_bytecode(source_code: &str, contract_name: &str) -> anyhow::Result<Vec<u8>> {
    let dir = tempdir().unwrap();
    let path = dir.path();
    let file_name = "test_code.sol";
    let test_code_path = path.join(file_name);
    let mut test_code_file = File::create(&test_code_path)?;
    writeln!(test_code_file, "{}", source_code)?;
    get_bytecode_path(path, file_name, contract_name)
}

#[tokio::test]
async fn test_fuel_for_counter_revm_application() -> anyhow::Result<()> {
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
    let module = get_bytecode(&source_code, "ExampleCounter")?;

    sol! {
        struct ConstructorArgs {
            uint256 initial_value;
        }
        function increment(uint256 input);
        function get_value();
    }

    let initial_value = U256::from(10000);
    let mut value = initial_value;
    let args = ConstructorArgs { initial_value };
    let instantiation_argument: Vec<u8> = args.abi_encode();

    let state = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        ..Default::default()
    };
    let mut view = state
        .into_view_with(ChainId::root(0), ExecutionRuntimeConfig::default())
        .await;
    let (app_desc, contract_blob, service_blob) = create_dummy_user_application_description(1);
    let app_id = From::from(&app_desc);

    let contract = EvmContractModule::Revm {
        module: module.clone(),
    };
    view.context()
        .extra()
        .user_contracts()
        .insert(app_id, contract.clone().into());

    let service = EvmServiceModule::Revm { module };
    view.context()
        .extra()
        .user_services()
        .insert(app_id, service.into());

    view.simulate_instantiation(
        contract.into(),
        Timestamp::from(2),
        app_desc,
        instantiation_argument,
        contract_blob,
        service_blob,
    )
    .await?;

    let operation_context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        round: Some(0),
        index: Some(0),
        authenticated_signer: None,
        authenticated_caller_id: None,
    };

    let query_context = QueryContext {
        chain_id: ChainId::root(0),
        next_block_height: BlockHeight(0),
        local_time: Timestamp::from(0),
    };

    let increments = [
        U256::from(2),
        U256::from(9),
        U256::from(7),
        U256::from(1000),
    ];
    let policy = ResourceControlPolicy {
        fuel_unit: Amount::from_attos(1),
        ..ResourceControlPolicy::default()
    };
    let amount = Amount::from_tokens(1);
    *view.system.balance.get_mut() = amount;
    let mut controller = ResourceController {
        policy: Arc::new(policy),
        tracker: ResourceTracker::default(),
        account: None,
    };

    for increment in &increments {
        let mut txn_tracker = TransactionTracker::new(0, 0, Some(Vec::new()));
        value += increment;
        let operation = incrementCall { input: *increment };
        let bytes = operation.abi_encode();
        let operation = Operation::User {
            application_id: app_id,
            bytes,
        };
        view.execute_operation(
            operation_context,
            Timestamp::from(0),
            operation,
            &mut txn_tracker,
            &mut controller,
        )
        .await?;

        let query = get_valueCall {};
        let bytes = query.abi_encode();
        let query = Query::User {
            application_id: app_id,
            bytes,
        };

        let result = view.query_application(query_context, query, None).await?;
        let QueryResponse::User(result) = result.response else {
            anyhow::bail!("Wrong QueryResponse result");
        };
        let result = U256::from_be_slice(&result);
        assert_eq!(result, value);
    }
    Ok(())
}
