// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, sync::Arc};

use ethers::{
    contract::abigen,
    prelude::{ContractFactory, SignerMiddleware},
    solc::Solc,
    types::U256,
};
use linera_ethereum::{
    client::{get_accounts_node, get_balance_node, get_block_number_node},
    test_utils::get_anvil,
};

abigen!(
    SimpleContract,
    "./contracts/SimpleToken.json",
    event_derives(serde::Deserialize, serde::Serialize)
);

#[tokio::test]
async fn test_get_accounts_balance() {
    let anvil_test = get_anvil().await.unwrap();
    let url = anvil_test.endpoint;
    let addresses = get_accounts_node(&url).await.unwrap();
    let block_nr = get_block_number_node(&url).await.unwrap();
    let target_balance = U256::from_dec_str("10000000000000000000000").unwrap();
    for address in addresses {
        let balance = get_balance_node(&url, &address, Some(block_nr))
            .await
            .unwrap();
        assert_eq!(balance, target_balance.0);
    }
}

#[tokio::test]
async fn test_contract() -> anyhow::Result<()> {
    // 1. Compile the code
    let source = Path::new(&env!("CARGO_MANIFEST_DIR")).join("contracts/simple_token.sol");
    let compiled = Solc::default()
        .compile_source(source)
        .expect("Could not compile contracts");

    // 2. Access to the contract that interests us
    let (abi, bytecode, _runtime_bytecode) = compiled
        .find("SimpleToken")
        .expect("could not find contract")
        .into_parts_or_default();

    // 3. Access to the wallets
    let anvil_test = get_anvil().await.unwrap();
    let (wallet0, addr0) = anvil_test.get_wallet(0);
    let (_wallet1, addr1) = anvil_test.get_wallet(1);

    // 4. instantiate the client with the wallet
    let client0 = SignerMiddleware::new(anvil_test.provider, wallet0);
    let client0 = Arc::new(client0);

    // 5. create a factory which will be used to deploy instances of the contract
    let factory = ContractFactory::new(abi, bytecode, client0.clone());

    // 6. deploy it with the constructor arguments, note the `legacy` call
    let initial_supply = U256::from_dec_str("1000")?;
    let contract = factory.deploy(initial_supply)?.legacy().send().await?;

    // 7. get the contract's address
    let addr_contract = contract.address();

    // 8. instantiate the contract
    let contract = SimpleContract::new(addr_contract, client0.clone());

    // 9. call the `setValue` method
    // (first `await` returns a PendingTransaction, second wait for the mining)
    let value = U256::from_dec_str("10")?;
    let _receipt = contract
        .transfer(addr1, value)
        .legacy()
        .send()
        .await?
        .await?;

    // 11. get the new value
    let value_read = contract.balance_of(addr1).call().await?;
    assert_eq!(value_read, value);
    let value_read = contract.balance_of(addr_contract).call().await?;
    assert_eq!(value_read, U256::from_dec_str("0")?);
    let value_read = contract.balance_of(addr0).call().await?;
    assert_eq!(value_read, U256::from_dec_str("990")?);
    // Add queries to the state of the chain.

    // Returning nothing
    Ok(())
}
