// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloy_sol_types::sol;

/// Solidity source for the LightClient contract.
pub const SOURCE: &str = include_str!("../solidity/LightClient.sol");

sol! {
    function addCommittee(
        bytes calldata data,
        bytes calldata committeeBlob,
        bytes[] calldata validators
    ) external;

    function verifyBlock(bytes calldata data) external view;

    function currentEpoch() external view returns (uint32);
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;
    use linera_base::{
        crypto::{CryptoHash, TestString, ValidatorSecretKey, ValidatorSignature},
        data_types::{BlockHeight, Epoch, Round},
    };
    use linera_chain::{block::ConfirmedBlock, data_types::Vote, types::ConfirmedBlockCertificate};
    use revm::{
        database::{CacheDB, EmptyDB},
        primitives::Address,
    };

    use super::{addCommitteeCall, currentEpochCall, verifyBlockCall};
    use crate::test_helpers::*;

    #[test]
    fn test_light_client_add_committee() {
        let mut light_client: TestLightClient = TestLightClient::new();
        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let call = light_client.add_committee_call(
            &new_public,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            call,
        );

        assert_eq!(light_client.query_current_epoch(), 1);
    }

    #[test]
    fn test_light_client_add_committee_rejects_wrong_blob() {
        let mut light_client = TestLightClient::new();
        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let mut call = light_client.add_committee_call(
            &new_public,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        call.committeeBlob = vec![0x01, 0x02, 0x03].into();

        assert!(
            try_call_contract(
                &mut light_client.db,
                light_client.deployer,
                light_client.contract,
                call
            )
            .is_err(),
            "should reject mismatched committee blob"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_non_sequential_epoch() {
        let mut light_client = TestLightClient::new();
        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let call = light_client.add_committee_call(
            &new_public,
            Epoch(5),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );

        assert!(
            try_call_contract(
                &mut light_client.db,
                light_client.deployer,
                light_client.contract,
                call
            )
            .is_err(),
            "should reject non-sequential epoch"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_wrong_chain() {
        let mut light_client = TestLightClient::new();
        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();
        let wrong_chain_id = CryptoHash::new(&TestString::new("other_chain"));

        let call = light_client.add_committee_call(
            &new_public,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            wrong_chain_id,
        );

        assert!(
            try_call_contract(
                &mut light_client.db,
                light_client.deployer,
                light_client.contract,
                call
            )
            .is_err(),
            "should reject CreateCommittee from non-admin chain"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_off_curve_key() {
        let mut light_client: TestLightClient = TestLightClient::new();
        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let mut call = light_client.add_committee_call(
            &new_public,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );

        // Construct a fake uncompressed key: correct x and y-parity, but y is
        // not the actual square root — the point is not on secp256k1.
        let mut fake_key = validator_uncompressed_key(&new_public);
        fake_key[32] ^= 0xFF; // corrupt y-coordinate while preserving parity
        call.validators = vec![fake_key.into()];

        assert!(
            try_call_contract(
                &mut light_client.db,
                light_client.deployer,
                light_client.contract,
                call
            )
            .is_err(),
            "should reject uncompressed key that is not on the secp256k1 curve"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_wrong_block_epoch() {
        let mut light_client = TestLightClient::new();

        // Transition to epoch 1
        let secret_1 = ValidatorSecretKey::generate();
        let public_1 = secret_1.public();
        let call_1 = light_client.add_committee_call(
            &public_1,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            call_1,
        );

        // Attempt epoch 2 transition with a block claiming epoch 0 (not current epoch 1).
        // Validator 0 is in epoch 0's committee so signature verification passes,
        // but the transition should be rejected because the block epoch is wrong.
        let secret_2 = ValidatorSecretKey::generate();
        let public_2 = secret_2.public();
        let call_2 = light_client.add_committee_call(
            &public_2,
            Epoch(2),
            Epoch::ZERO, // wrong: should be Epoch(1)
            BlockHeight(2),
            test_admin_chain_id(),
        );

        assert!(
            try_call_contract(
                &mut light_client.db,
                light_client.deployer,
                light_client.contract,
                call_2
            )
            .is_err(),
            "should reject committee transition from wrong epoch block"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_substituted_keys() {
        let mut light_client: TestLightClient = TestLightClient::new();

        let real_secret = ValidatorSecretKey::generate();
        let real_public = real_secret.public();

        let attacker_secret = ValidatorSecretKey::generate();
        let attacker_public = attacker_secret.public();

        let mut call = light_client.add_committee_call(
            &real_public,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        // Substitute the attacker's uncompressed key
        call.validators = vec![validator_uncompressed_key(&attacker_public).into()];

        assert!(
            try_call_contract(
                &mut light_client.db,
                light_client.deployer,
                light_client.contract,
                call
            )
            .is_err(),
            "should reject substituted keys that don't match the blob"
        );
    }

    #[test]
    fn test_light_client_verify_block() {
        let mut light_client: TestLightClient = TestLightClient::new();

        let certificate = create_signed_certificate(&light_client.secret, &light_client.public);
        let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

        light_client.verify_block(bcs_bytes);
    }

    #[test]
    fn test_light_client_rejects_duplicate_signer() {
        // This test needs a two-validator setup, so it can't use TestLightClient.
        let secret_a = ValidatorSecretKey::generate();
        let public_a = secret_a.public();
        let addr_a = validator_evm_address(&public_a);

        let secret_b = ValidatorSecretKey::generate();
        let public_b = secret_b.public();
        let addr_b = validator_evm_address(&public_b);

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract = deploy_light_client(
            &mut db,
            deployer,
            &[addr_a, addr_b],
            &[1, 1],
            test_admin_chain_id(),
            0,
        );

        // Only validator A signs, but duplicates the signature to try to reach quorum
        let chain_id = CryptoHash::new(&TestString::new("test_chain"));
        let block = create_test_block(chain_id, Epoch::ZERO, BlockHeight(1), vec![]);
        let confirmed = ConfirmedBlock::new(block);
        let vote = Vote::new(confirmed.clone(), Round::Fast, &secret_a);
        let certificate = ConfirmedBlockCertificate::new(
            confirmed,
            Round::Fast,
            vec![(public_a, vote.signature), (public_a, vote.signature)],
        );
        let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

        assert!(
            try_call_contract(
                &mut db,
                deployer,
                contract,
                verifyBlockCall {
                    data: bcs_bytes.into(),
                },
            )
            .is_err(),
            "should reject duplicate signer"
        );
    }

    #[test]
    fn test_light_client_rejects_wrong_epoch_committee() {
        let mut light_client: TestLightClient = TestLightClient::new();

        // Transition to epoch 1
        let secret_1 = ValidatorSecretKey::generate();
        let public_1 = secret_1.public();
        let call_1 = light_client.add_committee_call(
            &public_1,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            call_1,
        );

        // Create a block claiming epoch 0 but signed by epoch 1's validator.
        // Should fail: verified against epoch 0's committee where validator 1 is not a member.
        let bad_block = create_test_block(
            CryptoHash::new(&TestString::new("test_chain")),
            Epoch::ZERO,
            BlockHeight(2),
            vec![],
        );
        let bad_bytes = sign_and_serialize(&secret_1, &public_1, bad_block);

        assert!(
            light_client.try_verify_block(bad_bytes).is_err(),
            "should reject block verified against wrong epoch's committee"
        );
    }

    #[test]
    fn test_light_client_rejects_invalid_signature() {
        let mut light_client = TestLightClient::new();

        // Sign with a different key than the one in the committee
        let wrong_secret = ValidatorSecretKey::generate();
        let wrong_public = wrong_secret.public();
        let certificate = create_signed_certificate(&wrong_secret, &wrong_public);
        let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

        assert!(
            light_client.try_verify_block(bcs_bytes).is_err(),
            "should reject certificate signed by unknown validator"
        );
    }

    #[test]
    fn test_light_client_rejects_malleable_signature() {
        let mut light_client = TestLightClient::new();

        // Create a valid certificate
        let chain_id = CryptoHash::new(&TestString::new("test_chain"));
        let block = create_test_block(chain_id, Epoch::ZERO, BlockHeight(1), vec![]);
        let confirmed = ConfirmedBlock::new(block);
        let vote = Vote::new(confirmed.clone(), Round::Fast, &light_client.secret);

        // Compute high-s malleable variant: s' = n - s
        let secp256k1_n = U256::from_be_slice(
            &hex::decode("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141")
                .unwrap(),
        );
        let sig_bytes = vote.signature.as_bytes();
        let s = U256::from_be_slice(&sig_bytes[32..64]);
        let high_s = secp256k1_n - s;
        let mut malleable_bytes = sig_bytes;
        malleable_bytes[32..64].copy_from_slice(&high_s.to_be_bytes::<32>());
        let malleable_sig = ValidatorSignature::from_slice(malleable_bytes)
            .expect("malleable signature construction failed");

        let certificate = ConfirmedBlockCertificate::new(
            confirmed,
            Round::Fast,
            vec![(light_client.public, malleable_sig)],
        );
        let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

        assert!(
            light_client.try_verify_block(bcs_bytes).is_err(),
            "should reject high-s malleable signature"
        );
    }

    #[test]
    fn test_light_client_rejects_out_of_range_r() {
        let mut light_client = TestLightClient::new();

        // Create a valid certificate, serialize it, then patch the signature's r value
        // directly in the BCS bytes to set r = N (out of range).
        let certificate = create_signed_certificate(&light_client.secret, &light_client.public);
        let sig_bytes = certificate.signatures().first().unwrap().1.as_bytes();
        let original_r = &sig_bytes[..32];

        let mut bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

        // Find the original r bytes in the serialized certificate
        let r_pos = bcs_bytes
            .windows(32)
            .position(|w| w == original_r)
            .expect("could not find signature r in BCS bytes");

        // Replace r with N (>= curve order, out of valid range)
        let secp256k1_n =
            hex::decode("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141")
                .unwrap();
        bcs_bytes[r_pos..r_pos + 32].copy_from_slice(&secp256k1_n);

        assert!(
            light_client.try_verify_block(bcs_bytes).is_err(),
            "should reject signature with r >= N"
        );
    }

    /// Common test state for LightClient tests with a single initial validator.
    struct TestLightClient {
        db: CacheDB<EmptyDB>,
        deployer: Address,
        secret: ValidatorSecretKey,
        public: linera_base::crypto::ValidatorPublicKey,
        contract: Address,
    }

    impl TestLightClient {
        fn new() -> Self {
            let mut db = CacheDB::default();
            let deployer = Address::ZERO;
            let secret = ValidatorSecretKey::generate();
            let public = secret.public();
            let addr = validator_evm_address(&public);
            let contract =
                deploy_light_client(&mut db, deployer, &[addr], &[1], test_admin_chain_id(), 0);

            Self {
                db,
                deployer,
                secret,
                public,
                contract,
            }
        }

        fn add_committee_call(
            &self,
            new_public: &linera_base::crypto::ValidatorPublicKey,
            new_epoch: Epoch,
            block_epoch: Epoch,
            height: BlockHeight,
            chain_id: CryptoHash,
        ) -> addCommitteeCall {
            create_add_committee_call(
                &self.secret,
                &self.public,
                new_public,
                new_epoch,
                block_epoch,
                height,
                chain_id,
            )
        }

        fn query_current_epoch(&mut self) -> u32 {
            let (epoch, _, _) = call_contract(
                &mut self.db,
                self.deployer,
                self.contract,
                currentEpochCall {},
            );
            epoch
        }

        fn verify_block(&mut self, data: Vec<u8>) {
            call_contract(
                &mut self.db,
                self.deployer,
                self.contract,
                verifyBlockCall { data: data.into() },
            );
        }

        fn try_verify_block(&mut self, data: Vec<u8>) -> Result<(), String> {
            try_call_contract(
                &mut self.db,
                self.deployer,
                self.contract,
                verifyBlockCall { data: data.into() },
            )
            .map(|_| ())
        }
    }

    /// Creates a signed `addCommitteeCall` for transitioning to a new epoch with a single
    /// new validator. The block is signed by `signer_secret`/`signer_public` (the current
    /// epoch's validator) and placed on the admin chain at the given block epoch and height.
    fn create_add_committee_call(
        signer_secret: &ValidatorSecretKey,
        signer_public: &linera_base::crypto::ValidatorPublicKey,
        new_public: &linera_base::crypto::ValidatorPublicKey,
        new_epoch: Epoch,
        block_epoch: Epoch,
        height: BlockHeight,
        chain_id: CryptoHash,
    ) -> addCommitteeCall {
        let (committee_bytes, blob_hash) = create_committee_blob(new_public);
        let transactions = create_committee_transaction(new_epoch, blob_hash);
        let block = create_test_block(chain_id, block_epoch, height, transactions);
        let bcs_bytes = sign_and_serialize(signer_secret, signer_public, block);
        let new_uncompressed = validator_uncompressed_key(new_public);
        addCommitteeCall {
            data: bcs_bytes.into(),
            committeeBlob: committee_bytes.into(),
            validators: vec![new_uncompressed.into()],
        }
    }

    #[test]
    fn test_light_client_add_committee_two_validators() {
        let mut light_client = TestLightClient::new();
        let new_secret1 = ValidatorSecretKey::generate();
        let new_public1 = new_secret1.public();
        let new_secret2 = ValidatorSecretKey::generate();
        let new_public2 = new_secret2.public();

        let call = create_add_committee_call_multi(
            &light_client.secret,
            &light_client.public,
            &[new_public1, new_public2],
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            call,
        );

        assert_eq!(light_client.query_current_epoch(), 1);
    }

    #[test]
    fn test_light_client_add_committee_mixed_account_keys() {
        use std::collections::BTreeMap;

        use linera_base::{crypto::AccountPublicKey, data_types::BlobContent};
        use linera_execution::{committee::ValidatorState, ResourceControlPolicy};

        let mut light_client = TestLightClient::new();
        let new_secret1 = ValidatorSecretKey::generate();
        let new_public1 = new_secret1.public();
        let new_secret2 = ValidatorSecretKey::generate();
        let new_public2 = new_secret2.public();

        // Create a committee with mixed account key types:
        // validator 1 gets Secp256k1 account key, validator 2 gets Ed25519 account key
        let validators: BTreeMap<_, _> = [
            (
                new_public1,
                ValidatorState {
                    network_address: "127.0.0.1:8080".to_string(),
                    votes: 1,
                    account_public_key: AccountPublicKey::Secp256k1(new_public1),
                },
            ),
            (
                new_public2,
                ValidatorState {
                    network_address: "127.0.0.1:8081".to_string(),
                    votes: 1,
                    account_public_key: AccountPublicKey::Ed25519(
                        linera_base::crypto::Ed25519PublicKey::test_key(1),
                    ),
                },
            ),
        ]
        .into_iter()
        .collect();
        let committee =
            linera_execution::Committee::new(validators, ResourceControlPolicy::default());
        let committee_bytes = bcs::to_bytes(&committee).expect("committee serialization failed");
        let blob_content = BlobContent::new_committee(committee_bytes.clone());
        let blob_hash = CryptoHash::new(&blob_content);

        let transactions = create_committee_transaction(Epoch(1), blob_hash);
        let block = create_test_block(
            test_admin_chain_id(),
            Epoch::ZERO,
            BlockHeight(1),
            transactions,
        );
        let bcs_bytes = sign_and_serialize(&light_client.secret, &light_client.public, block);

        // Extract uncompressed keys in BTreeMap iteration order (which may differ
        // from BCS blob order). The contract handles arbitrary key ordering.
        let deserialized: linera_execution::Committee =
            bcs::from_bytes(&committee_bytes).expect("committee deserialization failed");
        let uncompressed_keys: Vec<alloy_primitives::Bytes> = deserialized
            .validators()
            .keys()
            .map(|pk| validator_uncompressed_key(pk).into())
            .collect();

        let call = addCommitteeCall {
            data: bcs_bytes.into(),
            committeeBlob: committee_bytes.into(),
            validators: uncompressed_keys,
        };
        call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            call,
        );

        assert_eq!(light_client.query_current_epoch(), 1);
    }

    /// Creates a committee blob with multiple validators and returns `(committee_bytes, blob_hash)`.
    fn create_multi_committee_blob(
        publics: &[linera_base::crypto::ValidatorPublicKey],
    ) -> (Vec<u8>, CryptoHash) {
        use std::collections::BTreeMap;

        use linera_base::{crypto::AccountPublicKey, data_types::BlobContent};
        use linera_execution::{committee::ValidatorState, ResourceControlPolicy};

        let validators: BTreeMap<_, _> = publics
            .iter()
            .enumerate()
            .map(|(i, public)| {
                (
                    *public,
                    ValidatorState {
                        network_address: format!("127.0.0.1:{}", 8080 + i),
                        votes: 1,
                        account_public_key: AccountPublicKey::Secp256k1(*public),
                    },
                )
            })
            .collect();
        let committee =
            linera_execution::Committee::new(validators, ResourceControlPolicy::default());
        let bytes = bcs::to_bytes(&committee).expect("committee serialization failed");
        let blob_content = BlobContent::new_committee(bytes.clone());
        let blob_hash = CryptoHash::new(&blob_content);
        (bytes, blob_hash)
    }

    /// Creates a signed `addCommitteeCall` for transitioning to a new epoch with multiple
    /// validators. The block is signed by `signer_secret`/`signer_public`.
    fn create_add_committee_call_multi(
        signer_secret: &ValidatorSecretKey,
        signer_public: &linera_base::crypto::ValidatorPublicKey,
        new_publics: &[linera_base::crypto::ValidatorPublicKey],
        new_epoch: Epoch,
        block_epoch: Epoch,
        height: BlockHeight,
        chain_id: CryptoHash,
    ) -> addCommitteeCall {
        let (committee_bytes, blob_hash) = create_multi_committee_blob(new_publics);
        let transactions = create_committee_transaction(new_epoch, blob_hash);
        let block = create_test_block(chain_id, block_epoch, height, transactions);
        let bcs_bytes = sign_and_serialize(signer_secret, signer_public, block);

        // Extract uncompressed keys in BTreeMap iteration order (which may differ
        // from BCS blob order). The contract handles arbitrary key ordering.
        let committee: linera_execution::Committee =
            bcs::from_bytes(&committee_bytes).expect("committee deserialization failed");
        let uncompressed_keys: Vec<alloy_primitives::Bytes> = committee
            .validators()
            .keys()
            .map(|pk| validator_uncompressed_key(pk).into())
            .collect();

        addCommitteeCall {
            data: bcs_bytes.into(),
            committeeBlob: committee_bytes.into(),
            validators: uncompressed_keys,
        }
    }

    /// Proptest: generates random secp256k1 keys and tests two-validator addCommittee.
    /// This reproduces non-deterministic "key x-coordinate mismatch" failures.
    mod proptest_add_committee {
        use proptest::prelude::*;
        use rand::SeedableRng;

        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(200))]

            #[test]
            fn two_validator_add_committee_succeeds(
                seed1 in any::<u64>(),
                seed2 in any::<u64>(),
                seed3 in any::<u64>(),
            ) {
                let mut rng1 = rand_chacha::ChaCha20Rng::seed_from_u64(seed1);
                let mut rng2 = rand_chacha::ChaCha20Rng::seed_from_u64(seed2);
                let mut rng3 = rand_chacha::ChaCha20Rng::seed_from_u64(seed3);

                let signer_secret = ValidatorSecretKey::generate_from(&mut rng1);
                let signer_public = signer_secret.public();
                let secret1 = ValidatorSecretKey::generate_from(&mut rng2);
                let public1 = secret1.public();
                let secret2 = ValidatorSecretKey::generate_from(&mut rng3);
                let public2 = secret2.public();

                // Skip if the two new keys happen to be the same
                if public1 == public2 {
                    return Ok(());
                }

                // Deploy light client with signer as initial validator
                let mut db = CacheDB::default();
                let deployer = Address::ZERO;
                let signer_addr = validator_evm_address(&signer_public);
                let contract = deploy_light_client(
                    &mut db, deployer, &[signer_addr], &[1],
                    test_admin_chain_id(), 0,
                );

                // Create two-validator committee and call addCommittee
                let call = create_add_committee_call_multi(
                    &signer_secret,
                    &signer_public,
                    &[public1, public2],
                    Epoch(1),
                    Epoch::ZERO,
                    BlockHeight(1),
                    test_admin_chain_id(),
                );

                let result = try_call_contract(&mut db, deployer, contract, call);
                prop_assert!(result.is_ok(), "addCommittee failed: {:?}", result.err());
            }
        }
    }
}
