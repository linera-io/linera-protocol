// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloy_sol_types::sol;

/// Solidity source for the LightClient contract.
pub const SOURCE: &str = include_str!("solidity/LightClient.sol");

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
    use revm::{database::CacheDB, primitives::Address};

    use super::{addCommitteeCall, currentEpochCall, verifyBlockCall};
    use crate::test_helpers::*;

    /// Deploys a light client with a single validator at epoch 0 using the test admin chain.
    fn deploy_single_validator_light_client(
        db: &mut CacheDB<revm::database::EmptyDB>,
        deployer: Address,
        public: &linera_base::crypto::ValidatorPublicKey,
    ) -> Address {
        let address = validator_evm_address(public);
        deploy_light_client(db, deployer, &[address], &[1], test_admin_chain_id())
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
    fn test_light_client_add_committee() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();

        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract = deploy_single_validator_light_client(&mut db, deployer, &public);

        let call = create_add_committee_call(
            &secret,
            &public,
            &new_public,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        call_contract(&mut db, deployer, contract, call);

        let current_epoch = call_contract(&mut db, deployer, contract, currentEpochCall {});
        assert_eq!(current_epoch, 1);
    }

    #[test]
    fn test_light_client_add_committee_rejects_wrong_blob() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();

        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract = deploy_single_validator_light_client(&mut db, deployer, &public);

        let mut call = create_add_committee_call(
            &secret,
            &public,
            &new_public,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        // Substitute a wrong blob
        call.committeeBlob = vec![0x01, 0x02, 0x03].into();

        assert!(
            try_call_contract(&mut db, deployer, contract, call).is_err(),
            "should reject mismatched committee blob"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_non_sequential_epoch() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();

        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract = deploy_single_validator_light_client(&mut db, deployer, &public);

        // CreateCommittee for epoch 5 (skipping 1-4)
        let call = create_add_committee_call(
            &secret,
            &public,
            &new_public,
            Epoch(5),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );

        assert!(
            try_call_contract(&mut db, deployer, contract, call).is_err(),
            "should reject non-sequential epoch"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_wrong_chain() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();

        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract = deploy_single_validator_light_client(&mut db, deployer, &public);

        let wrong_chain_id = CryptoHash::new(&TestString::new("other_chain"));
        let call = create_add_committee_call(
            &secret,
            &public,
            &new_public,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            wrong_chain_id,
        );

        assert!(
            try_call_contract(&mut db, deployer, contract, call).is_err(),
            "should reject CreateCommittee from non-admin chain"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_off_curve_key() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();

        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract = deploy_single_validator_light_client(&mut db, deployer, &public);

        let mut call = create_add_committee_call(
            &secret,
            &public,
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
            try_call_contract(&mut db, deployer, contract, call).is_err(),
            "should reject uncompressed key that is not on the secp256k1 curve"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_wrong_block_epoch() {
        let secret_0 = ValidatorSecretKey::generate();
        let public_0 = secret_0.public();

        let secret_1 = ValidatorSecretKey::generate();
        let public_1 = secret_1.public();

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract = deploy_single_validator_light_client(&mut db, deployer, &public_0);

        // Transition to epoch 1
        let call_1 = create_add_committee_call(
            &secret_0,
            &public_0,
            &public_1,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        call_contract(&mut db, deployer, contract, call_1);

        // Attempt epoch 2 transition with a block claiming epoch 0 (not current epoch 1).
        // Validator 0 is in epoch 0's committee so signature verification passes,
        // but the transition should be rejected because the block epoch is wrong.
        let secret_2 = ValidatorSecretKey::generate();
        let public_2 = secret_2.public();
        let call_2 = create_add_committee_call(
            &secret_0,
            &public_0,
            &public_2,
            Epoch(2),
            Epoch::ZERO, // wrong: should be Epoch(1)
            BlockHeight(2),
            test_admin_chain_id(),
        );

        assert!(
            try_call_contract(&mut db, deployer, contract, call_2).is_err(),
            "should reject committee transition from wrong epoch block"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_substituted_keys() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();

        let real_secret = ValidatorSecretKey::generate();
        let real_public = real_secret.public();

        let attacker_secret = ValidatorSecretKey::generate();
        let attacker_public = attacker_secret.public();

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract = deploy_single_validator_light_client(&mut db, deployer, &public);

        let mut call = create_add_committee_call(
            &secret,
            &public,
            &real_public,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        // Substitute the attacker's uncompressed key
        call.validators = vec![validator_uncompressed_key(&attacker_public).into()];

        assert!(
            try_call_contract(&mut db, deployer, contract, call).is_err(),
            "should reject substituted keys that don't match the blob"
        );
    }

    #[test]
    fn test_light_client_verify_block() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();

        let certificate = create_signed_certificate(&secret, &public);
        let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract = deploy_single_validator_light_client(&mut db, deployer, &public);

        call_contract(
            &mut db,
            deployer,
            contract,
            verifyBlockCall {
                data: bcs_bytes.into(),
            },
        );
    }

    #[test]
    fn test_light_client_rejects_duplicate_signer() {
        // Two validators, each with weight=1. Quorum = 2*2/3+1 = 2, so both must sign.
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
        let secret_0 = ValidatorSecretKey::generate();
        let public_0 = secret_0.public();

        let secret_1 = ValidatorSecretKey::generate();
        let public_1 = secret_1.public();

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract = deploy_single_validator_light_client(&mut db, deployer, &public_0);

        // Transition to epoch 1
        let call_1 = create_add_committee_call(
            &secret_0,
            &public_0,
            &public_1,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        call_contract(&mut db, deployer, contract, call_1);

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
            try_call_contract(
                &mut db,
                deployer,
                contract,
                verifyBlockCall {
                    data: bad_bytes.into(),
                },
            )
            .is_err(),
            "should reject block verified against wrong epoch's committee"
        );
    }

    #[test]
    fn test_light_client_rejects_invalid_signature() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();

        // Sign with a different key than the one in the committee
        let wrong_secret = ValidatorSecretKey::generate();
        let wrong_public = wrong_secret.public();
        let certificate = create_signed_certificate(&wrong_secret, &wrong_public);
        let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract = deploy_single_validator_light_client(&mut db, deployer, &public);

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
            "should reject certificate signed by unknown validator"
        );
    }

    #[test]
    fn test_light_client_rejects_malleable_signature() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract = deploy_single_validator_light_client(&mut db, deployer, &public);

        // Create a valid certificate
        let chain_id = CryptoHash::new(&TestString::new("test_chain"));
        let block = create_test_block(chain_id, Epoch::ZERO, BlockHeight(1), vec![]);
        let confirmed = ConfirmedBlock::new(block);
        let vote = Vote::new(confirmed.clone(), Round::Fast, &secret);

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

        let certificate =
            ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(public, malleable_sig)]);
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
            "should reject high-s malleable signature"
        );
    }
}
