// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use ethers::types::U256;
use linera_ethereum::{
    client::EthereumEndpoint,
    common::{EthereumDataType, EthereumEvent},
    test_utils::{get_anvil, get_test_contract_endpoints, ContractEndpoints},
};

#[tokio::test]
async fn test_get_accounts_balance() {
    let anvil_test = get_anvil().await.unwrap();
    let ethereum_endpoint = anvil_test.ethereum_endpoint;
    let addresses = ethereum_endpoint.get_accounts().await.unwrap();
    let block_nr = ethereum_endpoint.get_block_number().await.unwrap();
    let target_balance = U256::from_dec_str("10000000000000000000000").unwrap();
    for address in addresses {
        let balance = ethereum_endpoint
            .get_balance(&address, Some(block_nr))
            .await
            .unwrap();
        assert_eq!(balance, target_balance);
    }
}

#[tokio::test]
async fn test_contract() -> anyhow::Result<()> {
    let contract_endpoints = get_test_contract_endpoints().await?;
    let ContractEndpoints {
        url,
        addr_contract,
        addr0,
        addr1,
        _instance,
    } = contract_endpoints;
    let ethereum_endpoint = EthereumEndpoint::new(url)?;
    // Test the conversion of the types
    let event_name_expanded = "Types(address indexed,address,uint256,uint64,int64,uint32,int32,uint16,int16,uint8,int8,bool)";
    let events = ethereum_endpoint
        .read_events(&addr_contract, event_name_expanded, 0)
        .await?;
    let big_value =
        U256::from_dec_str("239675476885367459284564394732743434463843674346373355625").unwrap();
    let target_event = EthereumEvent {
        values: vec![
            EthereumDataType::Address(addr0.clone()),
            EthereumDataType::Address(addr0.clone()),
            EthereumDataType::Uint256(big_value),
            EthereumDataType::Uint64(4611686018427387904),
            EthereumDataType::Int64(-1152921504606846976),
            EthereumDataType::Uint32(1073726139),
            EthereumDataType::Int32(-1072173379),
            EthereumDataType::Uint16(16261),
            EthereumDataType::Int16(-16249),
            EthereumDataType::Uint8(135),
            EthereumDataType::Int8(-120),
            EthereumDataType::Bool(true),
        ],
        block_number: 1,
    };
    for event in events {
        assert_eq!(event, target_event);
    }
    // Test the Transfer entries
    let event_name_expanded = "Transfer(address indexed,address indexed,uint256)";
    let events = ethereum_endpoint
        .read_events(&addr_contract, event_name_expanded, 0)
        .await?;
    let value = U256::from_dec_str("10").unwrap();
    let target_event = EthereumEvent {
        values: vec![
            EthereumDataType::Address(addr0),
            EthereumDataType::Address(addr1),
            EthereumDataType::Uint256(value),
        ],
        block_number: 2,
    };
    for event in events {
        assert_eq!(event, target_event);
    }
    // Returning nothing
    Ok(())
}
