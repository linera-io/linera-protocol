// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "ethereum")]
use {
    alloy::primitives::U256,
    linera_ethereum::{
        client::EthereumQueries,
        common::{EthereumDataType, EthereumEvent},
        provider::EthereumClientSimplified,
        test_utils::{get_anvil, EventNumericsContractFunction, SimpleTokenContractFunction},
    },
    std::{collections::BTreeSet, str::FromStr},
};

#[cfg(feature = "ethereum")]
#[tokio::test]
async fn test_get_accounts_balance() -> anyhow::Result<()> {
    let anvil_test = get_anvil().await?;
    let ethereum_client = anvil_test.ethereum_client;
    let ethereum_client_simp = EthereumClientSimplified::new(anvil_test.endpoint);
    let addresses = ethereum_client
        .get_accounts()
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let addresses_simp = ethereum_client_simp
        .get_accounts()
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(addresses, addresses_simp);
    let block_number = ethereum_client.get_block_number().await?;
    let block_number_simp = ethereum_client_simp.get_block_number().await?;
    assert_eq!(block_number, block_number_simp);
    let target_balance = U256::from_str("10000000000000000000000")?;
    for address in addresses {
        let balance = ethereum_client.get_balance(&address, block_number).await?;
        let balance_simp = ethereum_client_simp
            .get_balance(&address, block_number)
            .await?;
        assert_eq!(balance, balance_simp);
        assert_eq!(balance, target_balance);
    }
    Ok(())
}

#[cfg(feature = "ethereum")]
#[tokio::test]
async fn test_event_numerics() -> anyhow::Result<()> {
    let anvil_test = get_anvil().await?;
    let ethereum_client_simp = EthereumClientSimplified::new(anvil_test.endpoint.clone());
    let event_numerics = EventNumericsContractFunction::new(anvil_test).await?;
    let contract_address = event_numerics.contract_address;

    // Test the conversion of the types
    let event_name_expanded = "Types(address indexed,address,uint256,uint64,int64,uint32,int32,uint16,int16,uint8,int8,bool)";
    let from_block = 0;
    let to_block = 2;
    let events = event_numerics
        .anvil_test
        .ethereum_client
        .read_events(&contract_address, event_name_expanded, from_block, to_block)
        .await?;
    let addr0 = event_numerics.anvil_test.get_address(0);
    let big_value = U256::from_str("239675476885367459284564394732743434463843674346373355625")?;
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
    assert_eq!(*events, [target_event.clone()]);
    let events = ethereum_client_simp
        .read_events(&contract_address, event_name_expanded, from_block, to_block)
        .await?;
    assert_eq!(*events, [target_event]);
    Ok(())
}

#[cfg(feature = "ethereum")]
#[tokio::test]
async fn test_simple_token_events() -> anyhow::Result<()> {
    let anvil_test = get_anvil().await?;
    let ethereum_client_simp = EthereumClientSimplified::new(anvil_test.endpoint.clone());
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
    simple_token.transfer(&addr0, &addr1, value).await?;
    let from_block = 0;
    let to_block = 3;

    // Test the Transfer entries
    let event_name_expanded = "Transfer(address indexed,address indexed,uint256)";
    let events = simple_token
        .anvil_test
        .ethereum_client
        .read_events(&contract_address, event_name_expanded, from_block, to_block)
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
    assert_eq!(*events, [target_event.clone()]);
    // Using the simplified client
    let events = ethereum_client_simp
        .read_events(&contract_address, event_name_expanded, from_block, to_block)
        .await?;
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
    simple_token.transfer(&addr0, &addr1, value).await?;

    // Checking the balances
    let block1 = 1;
    let balance0 = simple_token.balance_of(&addr0, block1).await?;
    assert_eq!(balance0, U256::from(1000));
    let balance1 = simple_token.balance_of(&addr1, block1).await?;
    assert_eq!(balance1, U256::from(0));

    // Checking the balances
    let block2 = 2;
    let balance0 = simple_token.balance_of(&addr0, block2).await?;
    assert_eq!(balance0, U256::from(990));
    let balance1 = simple_token.balance_of(&addr1, block2).await?;
    assert_eq!(balance1, U256::from(10));
    let balance_contract = simple_token.balance_of(&contract_address, block2).await?;
    assert_eq!(balance_contract, U256::from(0));
    Ok(())
}
