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
    use revm::{
        database::{CacheDB, EmptyDB},
        primitives::Address,
    };

    use super::{addBlockCall, chainIdCall, latestHeightCall, lightClientCall};
    use crate::test_helpers::*;

    sol! {
        function blockCount() external view returns (uint64);
    }

    #[test]
    fn test_microchain_add_block() {
        let mut microchain = TestMicrochain::new();

        let (light_client_address, _, _) = call_contract(
            &mut microchain.db,
            microchain.deployer,
            microchain.contract,
            lightClientCall {},
        );
        // Sanity check: the light client was deployed at a non-zero address
        assert_ne!(
            light_client_address,
            Address::ZERO,
            "light client address should be set"
        );

        let (chain_id_bytes, _, _) = call_contract(
            &mut microchain.db,
            microchain.deployer,
            microchain.contract,
            chainIdCall {},
        );
        assert_eq!(
            &chain_id_bytes,
            microchain.chain_id.as_bytes(),
            "chain ID should match"
        );

        microchain.add_block(BlockHeight(1));

        assert_eq!(microchain.query_block_count(), 1, "block count should be 1");
        assert_eq!(
            microchain.query_latest_height(),
            1,
            "latest height should be 1"
        );
    }

    #[test]
    fn test_microchain_rejects_duplicate_block() {
        let mut microchain = TestMicrochain::new();

        microchain.add_block(BlockHeight(1));

        assert!(
            microchain
                .try_add_block(microchain.chain_id, BlockHeight(1))
                .is_err(),
            "should reject duplicate block"
        );
    }

    #[test]
    fn test_microchain_rejects_wrong_chain_id() {
        let mut microchain = TestMicrochain::new();
        let wrong_chain_id = CryptoHash::new(&TestString::new("wrong_chain"));

        assert!(
            microchain
                .try_add_block(wrong_chain_id, BlockHeight(1))
                .is_err(),
            "should reject block from wrong chain"
        );
    }

    #[test]
    fn test_microchain_rejects_non_sequential_height() {
        let mut t = TestMicrochain::new();

        assert!(
            t.try_add_block(t.chain_id, BlockHeight(5)).is_err(),
            "should reject non-sequential block height"
        );
    }

    /// Common test state for Microchain tests.
    struct TestMicrochain {
        db: CacheDB<EmptyDB>,
        deployer: Address,
        secret: ValidatorSecretKey,
        public: linera_base::crypto::ValidatorPublicKey,
        chain_id: CryptoHash,
        contract: Address,
    }

    impl TestMicrochain {
        fn new() -> Self {
            let mut db = CacheDB::default();
            let deployer = Address::ZERO;
            let secret = ValidatorSecretKey::generate();
            let public = secret.public();
            let address = validator_evm_address(&public);
            let chain_id = CryptoHash::new(&TestString::new("test_chain"));
            let light_client = deploy_light_client(
                &mut db,
                deployer,
                &[address],
                &[1],
                test_admin_chain_id(),
                0,
            );
            let contract = deploy_microchain(&mut db, deployer, light_client, chain_id, 0);

            Self {
                db,
                deployer,
                secret,
                public,
                chain_id,
                contract,
            }
        }

        fn add_block(&mut self, height: BlockHeight) {
            let cert = create_signed_certificate_for_chain(
                &self.secret,
                &self.public,
                self.chain_id,
                height,
            );
            let bcs_bytes = bcs::to_bytes(&cert).expect("BCS serialization failed");
            call_contract(
                &mut self.db,
                self.deployer,
                self.contract,
                addBlockCall {
                    data: bcs_bytes.into(),
                },
            );
        }

        fn try_add_block(
            &mut self,
            chain_id: CryptoHash,
            height: BlockHeight,
        ) -> Result<(), String> {
            let cert =
                create_signed_certificate_for_chain(&self.secret, &self.public, chain_id, height);
            let bcs_bytes = bcs::to_bytes(&cert).expect("BCS serialization failed");
            try_call_contract(
                &mut self.db,
                self.deployer,
                self.contract,
                addBlockCall {
                    data: bcs_bytes.into(),
                },
            )
            .map(|_| ())
        }

        fn query_block_count(&mut self) -> u64 {
            let (count, _, _) = call_contract(
                &mut self.db,
                self.deployer,
                self.contract,
                blockCountCall {},
            );
            count
        }

        fn query_latest_height(&mut self) -> u64 {
            let (height, _, _) = call_contract(
                &mut self.db,
                self.deployer,
                self.contract,
                latestHeightCall {},
            );
            height
        }
    }
}
