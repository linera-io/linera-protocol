// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "ethereum")]
use {
    std::str::FromStr,
    alloy_primitives::U256,
    linera_ethereum::{
        common::{EthereumDataType, EthereumEvent},
        test_utils::{get_anvil, EventNumericsContractFunction, SimpleTokenContractFunction},
    },
};

#[cfg(feature = "ethereum")]
#[tokio::test]
async fn test_get_accounts_balance() -> anyhow::Result<()> {
    let anvil_test = get_anvil().await?;
    let ethereum_endpoint = anvil_test.ethereum_endpoint;
    let addresses = ethereum_endpoint.get_accounts().await?;
    let block_nr = ethereum_endpoint.get_block_number().await?;
    let target_balance = U256::from_str("10000000000000000000000")?;
    for address in addresses {
        let balance = ethereum_endpoint
            .get_balance(&address, Some(block_nr))
            .await?;
        assert_eq!(balance, target_balance);
    }
    Ok(())
}

#[cfg(feature = "ethereum")]
#[tokio::test]
async fn test_event_numerics() -> anyhow::Result<()> {
    let anvil_test = get_anvil().await?;
    let event_numerics = EventNumericsContractFunction::new(anvil_test).await?;
    let contract_address = event_numerics.contract_address;

    // Test the conversion of the types
    let event_name_expanded = "Types(address indexed,address,uint256,uint64,int64,uint32,int32,uint16,int16,uint8,int8,bool)";
    let events = event_numerics
        .anvil_test
        .ethereum_endpoint
        .read_events(&contract_address, event_name_expanded, 0)
        .await?;
    let addr0 = event_numerics.anvil_test.get_address(0);
    let big_value =
        U256::from_str("239675476885367459284564394732743434463843674346373355625")?;
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
    assert_eq!(*events, [target_event]);
    Ok(())
}

#[cfg(feature = "ethereum")]
#[tokio::test]
async fn test_simple_token_events() -> anyhow::Result<()> {
    let anvil_test = get_anvil().await?;
    let simple_token = SimpleTokenContractFunction::new(anvil_test).await?;
    let contract_address = simple_token.contract_address.clone();
    let addr0 = simple_token.anvil_test.get_address(0);
    let addr1 = simple_token.anvil_test.get_address(1);
    println!("addr0={}", addr0);
    println!("addr1={}", addr1);

    let balance0 = simple_token.balance_of(&addr0).await?;
    assert_eq!(balance0, U256::from(1000));
    let balance1 = simple_token.balance_of(&addr1).await?;
    assert_eq!(balance1, U256::from(0));

    
    // Doing the transfer
    // We have to use a direct call since only non-executive operation
    // are done in the client. So, we use a direct call.
    // First await returns a PendingTransaction and the second does the
    // mining.
    let value = U256::from(10);
    println!("Before the simple_token.transfer");
    simple_token.transfer(&addr1, value).await?;
    println!("After the simple_token.transfer");

    // Test the Transfer entries
    let event_name_expanded = "Transfer(address indexed,address indexed,uint256)";
    let events = simple_token
        .anvil_test
        .ethereum_endpoint
        .read_events(&contract_address, event_name_expanded, 0)
        .await?;
    let value = U256::from(10);
    let target_event = EthereumEvent {
        values: vec![
            EthereumDataType::Address(addr0),
            EthereumDataType::Address(addr1),
            EthereumDataType::Uint256(value),
        ],
        block_number: 2,
    };
    assert_eq!(*events, [target_event]);
    Ok(())
}

#[cfg(feature = "ethereum")]
#[tokio::test]
async fn test_simple_token_queries() -> anyhow::Result<()> {
    let anvil_test = get_anvil().await?;
    let simple_token = SimpleTokenContractFunction::new(anvil_test).await?;
    let contract_address = simple_token.contract_address.clone();

    let addr0 = simple_token.anvil_test.get_address(0);
    let addr1 = simple_token.anvil_test.get_address(1);

    // Doing the transfer
    // We have to use a direct call since only non-executive operation
    // are done in the client. So, we use a direct call.
    // First await returns a PendingTransaction and the second does the
    // mining.
    let value = U256::from(10);
    simple_token.transfer(&addr1, value).await?;

    // Checking the balances
    let balance0 = simple_token.balance_of(&addr0).await?;
    assert_eq!(balance0, U256::from(990));
    let balance1 = simple_token.balance_of(&addr1).await?;
    assert_eq!(balance1, U256::from(10));
    let balance_contract = simple_token.balance_of(&contract_address).await?;
    assert_eq!(balance_contract, U256::from(0));
    Ok(())
}
