// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloy_sol_types::sol;

/// Solidity source for the Microchain abstract contract.
pub const SOURCE: &str = include_str!("solidity/Microchain.sol");

sol! {
    function addBlock(bytes calldata data) external;

    function latestHeight() external view returns (uint64);

    function lightClient() external view returns (address);

    function chainId() external view returns (bytes32);
}

#[cfg(test)]
mod tests {
    use alloy_sol_types::sol;
    use linera_base::{
        crypto::{CryptoHash, TestString, ValidatorSecretKey},
        data_types::BlockHeight,
    };
    use revm::{database::CacheDB, primitives::Address};

    use super::{addBlockCall, chainIdCall, latestHeightCall, lightClientCall};
    use crate::test_helpers::*;

    sol! {
        function blockCount() external view returns (uint64);
    }

    #[test]
    fn test_microchain_add_block() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let address = validator_evm_address(&public);

        let chain_id = CryptoHash::new(&TestString::new("test_chain"));

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let light_client =
            deploy_light_client(&mut db, deployer, &[address], &[1], test_admin_chain_id());
        let microchain = deploy_microchain(&mut db, deployer, light_client, chain_id);

        let (light_client_address, _, _) =
            call_contract(&mut db, deployer, microchain, lightClientCall {});
        assert_eq!(
            light_client_address, light_client,
            "light client address should match"
        );

        let (chain_id_bytes, _, _) = call_contract(&mut db, deployer, microchain, chainIdCall {});
        assert_eq!(
            &chain_id_bytes,
            chain_id.as_bytes(),
            "chain ID should match"
        );

        // Add block at height 1
        let cert1 = create_signed_certificate_for_chain(&secret, &public, chain_id, BlockHeight(1));
        let bcs1 = bcs::to_bytes(&cert1).expect("BCS serialization failed");
        call_contract(
            &mut db,
            deployer,
            microchain,
            addBlockCall { data: bcs1.into() },
        );

        // Verify state
        let (count, _, _) = call_contract(&mut db, deployer, microchain, blockCountCall {});
        assert_eq!(count, 1, "block count should be 1");

        let (height, _, _) = call_contract(&mut db, deployer, microchain, latestHeightCall {});
        assert_eq!(height, 1, "latest height should be 1");
    }

    #[test]
    fn test_microchain_rejects_wrong_chain_id() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let address = validator_evm_address(&public);

        let chain_id = CryptoHash::new(&TestString::new("test_chain"));
        let wrong_chain_id = CryptoHash::new(&TestString::new("wrong_chain"));

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let light_client =
            deploy_light_client(&mut db, deployer, &[address], &[1], test_admin_chain_id());
        let microchain = deploy_microchain(&mut db, deployer, light_client, chain_id);

        // Try to add a block from a different chain
        let cert =
            create_signed_certificate_for_chain(&secret, &public, wrong_chain_id, BlockHeight(1));
        let bcs_bytes = bcs::to_bytes(&cert).expect("BCS serialization failed");
        assert!(
            try_call_contract(
                &mut db,
                deployer,
                microchain,
                addBlockCall {
                    data: bcs_bytes.into(),
                },
            )
            .is_err(),
            "should reject block from wrong chain"
        );
    }

    #[test]
    fn test_microchain_rejects_non_sequential_height() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let address = validator_evm_address(&public);

        let chain_id = CryptoHash::new(&TestString::new("test_chain"));

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let light_client =
            deploy_light_client(&mut db, deployer, &[address], &[1], test_admin_chain_id());
        let microchain = deploy_microchain(&mut db, deployer, light_client, chain_id);

        // Try to add block at height 5 (skipping 1-4)
        let cert = create_signed_certificate_for_chain(&secret, &public, chain_id, BlockHeight(5));
        let bcs_bytes = bcs::to_bytes(&cert).expect("BCS serialization failed");
        assert!(
            try_call_contract(
                &mut db,
                deployer,
                microchain,
                addBlockCall {
                    data: bcs_bytes.into(),
                },
            )
            .is_err(),
            "should reject non-sequential block height"
        );
    }
}
