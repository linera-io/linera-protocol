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
    use std::collections::BTreeMap;

    use linera_base::{
        crypto::{AccountPublicKey, CryptoHash, TestString, ValidatorSecretKey},
        data_types::{BlobContent, BlockHeight, Epoch, Round},
    };
    use linera_chain::{
        block::ConfirmedBlock,
        data_types::{Transaction, Vote},
        types::ConfirmedBlockCertificate,
    };
    use linera_execution::{
        committee::ValidatorState, system::AdminOperation, Operation, ResourceControlPolicy,
        SystemOperation,
    };
    use revm::{database::CacheDB, primitives::Address};

    use super::{addCommitteeCall, currentEpochCall, verifyBlockCall};
    use crate::test_helpers::*;

    /// The admin chain ID used in tests.
    fn test_admin_chain_id() -> CryptoHash {
        CryptoHash::new(&TestString::new("admin_chain"))
    }

    #[test]
    fn test_light_client_add_committee() {
        // Current validator (epoch 0)
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let address = validator_evm_address(&public);

        // New validator (epoch 1)
        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        // Create a committee blob for the new epoch
        let new_committee = linera_execution::Committee::new(
            BTreeMap::from([(
                new_public,
                ValidatorState {
                    network_address: "127.0.0.1:8080".to_string(),
                    votes: 1,
                    account_public_key: AccountPublicKey::Secp256k1(new_public),
                },
            )]),
            ResourceControlPolicy::default(),
        );
        let committee_bytes =
            bcs::to_bytes(&new_committee).expect("committee serialization failed");
        let blob_content = BlobContent::new_committee(committee_bytes.clone());
        let blob_hash = CryptoHash::new(&blob_content);

        // Create a block with CreateCommittee operation
        let new_epoch = Epoch(1);
        let transactions = vec![Transaction::ExecuteOperation(Operation::System(Box::new(
            SystemOperation::Admin(AdminOperation::CreateCommittee {
                epoch: new_epoch,
                blob_hash,
            }),
        )))];
        let block = create_test_block(
            test_admin_chain_id(),
            Epoch::ZERO,
            BlockHeight(1),
            transactions,
        );
        let confirmed = ConfirmedBlock::new(block);
        let vote = Vote::new(confirmed.clone(), Round::Fast, &secret);
        let certificate =
            ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(public, vote.signature)]);
        let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract =
            deploy_light_client(&mut db, deployer, &[address], &[1], test_admin_chain_id());

        let new_uncompressed = validator_uncompressed_key(&new_public);

        call_contract(
            &mut db,
            deployer,
            contract,
            addCommitteeCall {
                data: bcs_bytes.into(),
                committeeBlob: committee_bytes.into(),
                validators: vec![new_uncompressed.into()],
            },
        );

        let current_epoch = call_contract(&mut db, deployer, contract, currentEpochCall {});
        assert_eq!(current_epoch, new_epoch.0);
    }

    #[test]
    fn test_light_client_add_committee_rejects_wrong_blob() {
        // Current validator (epoch 0)
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let address = validator_evm_address(&public);

        // Create a committee blob and compute its hash
        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let new_committee = linera_execution::Committee::new(
            BTreeMap::from([(
                new_public,
                ValidatorState {
                    network_address: "127.0.0.1:8080".to_string(),
                    votes: 1,
                    account_public_key: AccountPublicKey::Secp256k1(new_public),
                },
            )]),
            ResourceControlPolicy::default(),
        );
        let committee_bytes =
            bcs::to_bytes(&new_committee).expect("committee serialization failed");
        let blob_content = BlobContent::new_committee(committee_bytes);
        let blob_hash = CryptoHash::new(&blob_content);

        // Create a block with CreateCommittee pointing to the real blob hash
        let transactions = vec![Transaction::ExecuteOperation(Operation::System(Box::new(
            SystemOperation::Admin(AdminOperation::CreateCommittee {
                epoch: Epoch(1),
                blob_hash,
            }),
        )))];
        let block = create_test_block(
            test_admin_chain_id(),
            Epoch::ZERO,
            BlockHeight(1),
            transactions,
        );
        let confirmed = ConfirmedBlock::new(block);
        let vote = Vote::new(confirmed.clone(), Round::Fast, &secret);
        let certificate =
            ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(public, vote.signature)]);
        let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract =
            deploy_light_client(&mut db, deployer, &[address], &[1], test_admin_chain_id());

        // Pass a different (wrong) committee blob
        let new_uncompressed = validator_uncompressed_key(&new_public);
        let wrong_blob = vec![0x01, 0x02, 0x03];
        assert!(
            try_call_contract(
                &mut db,
                deployer,
                contract,
                addCommitteeCall {
                    data: bcs_bytes.into(),
                    committeeBlob: wrong_blob.into(),
                    validators: vec![new_uncompressed.into()],
                },
            )
            .is_err(),
            "should reject mismatched committee blob"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_non_sequential_epoch() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let address = validator_evm_address(&public);

        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let new_committee = linera_execution::Committee::new(
            BTreeMap::from([(
                new_public,
                ValidatorState {
                    network_address: "127.0.0.1:8080".to_string(),
                    votes: 1,
                    account_public_key: AccountPublicKey::Secp256k1(new_public),
                },
            )]),
            ResourceControlPolicy::default(),
        );
        let committee_bytes =
            bcs::to_bytes(&new_committee).expect("committee serialization failed");
        let blob_content = BlobContent::new_committee(committee_bytes.clone());
        let blob_hash = CryptoHash::new(&blob_content);

        // Create a block with CreateCommittee for epoch 5 (skipping epochs 1-4)
        let transactions = vec![Transaction::ExecuteOperation(Operation::System(Box::new(
            SystemOperation::Admin(AdminOperation::CreateCommittee {
                epoch: Epoch(5),
                blob_hash,
            }),
        )))];
        let block = create_test_block(
            test_admin_chain_id(),
            Epoch::ZERO,
            BlockHeight(1),
            transactions,
        );
        let confirmed = ConfirmedBlock::new(block);
        let vote = Vote::new(confirmed.clone(), Round::Fast, &secret);
        let certificate =
            ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(public, vote.signature)]);
        let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract =
            deploy_light_client(&mut db, deployer, &[address], &[1], test_admin_chain_id());

        let new_uncompressed = validator_uncompressed_key(&new_public);
        assert!(
            try_call_contract(
                &mut db,
                deployer,
                contract,
                addCommitteeCall {
                    data: bcs_bytes.into(),
                    committeeBlob: committee_bytes.into(),
                    validators: vec![new_uncompressed.into()],
                },
            )
            .is_err(),
            "should reject non-sequential epoch"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_wrong_chain() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let address = validator_evm_address(&public);

        let admin_chain_id = CryptoHash::new(&TestString::new("admin_chain"));
        let wrong_chain_id = CryptoHash::new(&TestString::new("other_chain"));

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract = deploy_light_client(&mut db, deployer, &[address], &[1], admin_chain_id);

        // New validator for epoch 1
        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let new_committee = linera_execution::Committee::new(
            BTreeMap::from([(
                new_public,
                ValidatorState {
                    network_address: "127.0.0.1:8080".to_string(),
                    votes: 1,
                    account_public_key: AccountPublicKey::Secp256k1(new_public),
                },
            )]),
            ResourceControlPolicy::default(),
        );
        let committee_bytes =
            bcs::to_bytes(&new_committee).expect("committee serialization failed");
        let blob_content = BlobContent::new_committee(committee_bytes.clone());
        let blob_hash = CryptoHash::new(&blob_content);

        // Create a valid CreateCommittee block but from the wrong chain
        let transactions = vec![Transaction::ExecuteOperation(Operation::System(Box::new(
            SystemOperation::Admin(AdminOperation::CreateCommittee {
                epoch: Epoch(1),
                blob_hash,
            }),
        )))];
        let block = create_test_block(
            wrong_chain_id, // not the admin chain
            Epoch::ZERO,
            BlockHeight(1),
            transactions,
        );
        let confirmed = ConfirmedBlock::new(block);
        let vote = Vote::new(confirmed.clone(), Round::Fast, &secret);
        let certificate =
            ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(public, vote.signature)]);
        let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");
        let new_uncompressed = validator_uncompressed_key(&new_public);

        assert!(
            try_call_contract(
                &mut db,
                deployer,
                contract,
                addCommitteeCall {
                    data: bcs_bytes.into(),
                    committeeBlob: committee_bytes.into(),
                    validators: vec![new_uncompressed.into()],
                },
            )
            .is_err(),
            "should reject CreateCommittee from non-admin chain"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_wrong_block_epoch() {
        // Epoch 0 validator
        let secret_0 = ValidatorSecretKey::generate();
        let public_0 = secret_0.public();
        let addr_0 = validator_evm_address(&public_0);

        // Epoch 1 validator
        let secret_1 = ValidatorSecretKey::generate();
        let public_1 = secret_1.public();

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract =
            deploy_light_client(&mut db, deployer, &[addr_0], &[1], test_admin_chain_id());

        // Transition to epoch 1 with validator 1
        let committee_1 = linera_execution::Committee::new(
            BTreeMap::from([(
                public_1,
                ValidatorState {
                    network_address: "127.0.0.1:8080".to_string(),
                    votes: 1,
                    account_public_key: AccountPublicKey::Secp256k1(public_1),
                },
            )]),
            ResourceControlPolicy::default(),
        );
        let committee_1_bytes =
            bcs::to_bytes(&committee_1).expect("committee serialization failed");
        let blob_content_1 = BlobContent::new_committee(committee_1_bytes.clone());
        let blob_hash_1 = CryptoHash::new(&blob_content_1);

        let txns = vec![Transaction::ExecuteOperation(Operation::System(Box::new(
            SystemOperation::Admin(AdminOperation::CreateCommittee {
                epoch: Epoch(1),
                blob_hash: blob_hash_1,
            }),
        )))];
        let block = create_test_block(test_admin_chain_id(), Epoch::ZERO, BlockHeight(1), txns);
        let confirmed = ConfirmedBlock::new(block);
        let vote = Vote::new(confirmed.clone(), Round::Fast, &secret_0);
        let cert = ConfirmedBlockCertificate::new(
            confirmed,
            Round::Fast,
            vec![(public_0, vote.signature)],
        );
        let cert_bytes = bcs::to_bytes(&cert).expect("BCS serialization failed");
        let uncompressed_1 = validator_uncompressed_key(&public_1);

        call_contract(
            &mut db,
            deployer,
            contract,
            addCommitteeCall {
                data: cert_bytes.into(),
                committeeBlob: committee_1_bytes.into(),
                validators: vec![uncompressed_1.into()],
            },
        );
        // Now currentEpoch == 1.

        // Prepare epoch 2 committee
        let secret_2 = ValidatorSecretKey::generate();
        let public_2 = secret_2.public();

        let committee_2 = linera_execution::Committee::new(
            BTreeMap::from([(
                public_2,
                ValidatorState {
                    network_address: "127.0.0.1:8080".to_string(),
                    votes: 1,
                    account_public_key: AccountPublicKey::Secp256k1(public_2),
                },
            )]),
            ResourceControlPolicy::default(),
        );
        let committee_2_bytes =
            bcs::to_bytes(&committee_2).expect("committee serialization failed");
        let blob_content_2 = BlobContent::new_committee(committee_2_bytes.clone());
        let blob_hash_2 = CryptoHash::new(&blob_content_2);

        // Create a block with CreateCommittee for epoch 2, but the block itself
        // claims epoch 0 (not the current epoch 1). Validator 0 is in epoch 0's
        // committee so signature verification passes — but the transition should
        // be rejected because the authorizing block is from the wrong epoch.
        let txns_2 = vec![Transaction::ExecuteOperation(Operation::System(Box::new(
            SystemOperation::Admin(AdminOperation::CreateCommittee {
                epoch: Epoch(2),
                blob_hash: blob_hash_2,
            }),
        )))];
        let stale_block = create_test_block(
            test_admin_chain_id(),
            Epoch::ZERO, // wrong: should be Epoch(1)
            BlockHeight(2),
            txns_2,
        );
        let stale_confirmed = ConfirmedBlock::new(stale_block);
        let stale_vote = Vote::new(stale_confirmed.clone(), Round::Fast, &secret_0);
        let stale_cert = ConfirmedBlockCertificate::new(
            stale_confirmed,
            Round::Fast,
            vec![(public_0, stale_vote.signature)],
        );
        let stale_bytes = bcs::to_bytes(&stale_cert).expect("BCS serialization failed");
        let uncompressed_2 = validator_uncompressed_key(&public_2);

        assert!(
            try_call_contract(
                &mut db,
                deployer,
                contract,
                addCommitteeCall {
                    data: stale_bytes.into(),
                    committeeBlob: committee_2_bytes.into(),
                    validators: vec![uncompressed_2.into()],
                },
            )
            .is_err(),
            "should reject committee transition from wrong epoch block"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_substituted_keys() {
        // Current validator (epoch 0)
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let address = validator_evm_address(&public);

        // Real validator for the new committee
        let real_secret = ValidatorSecretKey::generate();
        let real_public = real_secret.public();

        // Attacker's key — different from the one in the blob
        let attacker_secret = ValidatorSecretKey::generate();
        let attacker_public = attacker_secret.public();
        let attacker_uncompressed = validator_uncompressed_key(&attacker_public);

        // Create a legitimate committee blob with the real validator
        let new_committee = linera_execution::Committee::new(
            BTreeMap::from([(
                real_public,
                ValidatorState {
                    network_address: "127.0.0.1:8080".to_string(),
                    votes: 1,
                    account_public_key: AccountPublicKey::Secp256k1(real_public),
                },
            )]),
            ResourceControlPolicy::default(),
        );
        let committee_bytes =
            bcs::to_bytes(&new_committee).expect("committee serialization failed");
        let blob_content = BlobContent::new_committee(committee_bytes.clone());
        let blob_hash = CryptoHash::new(&blob_content);

        // Create a valid certified block with CreateCommittee
        let transactions = vec![Transaction::ExecuteOperation(Operation::System(Box::new(
            SystemOperation::Admin(AdminOperation::CreateCommittee {
                epoch: Epoch(1),
                blob_hash,
            }),
        )))];
        let block = create_test_block(
            test_admin_chain_id(),
            Epoch::ZERO,
            BlockHeight(1),
            transactions,
        );
        let confirmed = ConfirmedBlock::new(block);
        let vote = Vote::new(confirmed.clone(), Round::Fast, &secret);
        let certificate =
            ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(public, vote.signature)]);
        let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract =
            deploy_light_client(&mut db, deployer, &[address], &[1], test_admin_chain_id());

        // Attack: pass a valid cert + blob but substitute the attacker's key
        assert!(
            try_call_contract(
                &mut db,
                deployer,
                contract,
                addCommitteeCall {
                    data: bcs_bytes.into(),
                    committeeBlob: committee_bytes.into(),
                    validators: vec![attacker_uncompressed.into()],
                },
            )
            .is_err(),
            "should reject substituted keys that don't match the blob"
        );
    }

    #[test]
    fn test_light_client_verify_block() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let address = validator_evm_address(&public);

        let certificate = create_signed_certificate(&secret, &public);
        let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract =
            deploy_light_client(&mut db, deployer, &[address], &[1], test_admin_chain_id());

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
        // Epoch 0 validator
        let secret_0 = ValidatorSecretKey::generate();
        let public_0 = secret_0.public();
        let addr_0 = validator_evm_address(&public_0);

        // Epoch 1 validator
        let secret_1 = ValidatorSecretKey::generate();
        let public_1 = secret_1.public();

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract =
            deploy_light_client(&mut db, deployer, &[addr_0], &[1], test_admin_chain_id());

        // Transition to epoch 1
        let new_committee = linera_execution::Committee::new(
            BTreeMap::from([(
                public_1,
                ValidatorState {
                    network_address: "127.0.0.1:8080".to_string(),
                    votes: 1,
                    account_public_key: AccountPublicKey::Secp256k1(public_1),
                },
            )]),
            ResourceControlPolicy::default(),
        );
        let committee_bytes =
            bcs::to_bytes(&new_committee).expect("committee serialization failed");
        let blob_content = BlobContent::new_committee(committee_bytes.clone());
        let blob_hash = CryptoHash::new(&blob_content);

        let transactions = vec![Transaction::ExecuteOperation(Operation::System(Box::new(
            SystemOperation::Admin(AdminOperation::CreateCommittee {
                epoch: Epoch(1),
                blob_hash,
            }),
        )))];
        let block = create_test_block(
            test_admin_chain_id(),
            Epoch::ZERO,
            BlockHeight(1),
            transactions,
        );
        let confirmed = ConfirmedBlock::new(block);
        let vote = Vote::new(confirmed.clone(), Round::Fast, &secret_0);
        let cert = ConfirmedBlockCertificate::new(
            confirmed,
            Round::Fast,
            vec![(public_0, vote.signature)],
        );
        let cert_bytes = bcs::to_bytes(&cert).expect("BCS serialization failed");
        let uncompressed_1 = validator_uncompressed_key(&public_1);

        call_contract(
            &mut db,
            deployer,
            contract,
            addCommitteeCall {
                data: cert_bytes.into(),
                committeeBlob: committee_bytes.into(),
                validators: vec![uncompressed_1.into()],
            },
        );

        // Now currentEpoch is 1.
        // Create a block claiming epoch 0 but signed by epoch 1's validator.
        // This should FAIL: the block says epoch 0 so it should be verified
        // against epoch 0's committee, where validator 1 is not a member.
        let bad_block = create_test_block(
            CryptoHash::new(&TestString::new("test_chain")),
            Epoch::ZERO, // claims epoch 0
            BlockHeight(2),
            vec![],
        );
        let bad_confirmed = ConfirmedBlock::new(bad_block);
        let bad_vote = Vote::new(bad_confirmed.clone(), Round::Fast, &secret_1);
        let bad_cert = ConfirmedBlockCertificate::new(
            bad_confirmed,
            Round::Fast,
            vec![(public_1, bad_vote.signature)],
        );
        let bad_bytes = bcs::to_bytes(&bad_cert).expect("BCS serialization failed");

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
        let address = validator_evm_address(&public);

        // Sign with a different key than the one in the committee
        let wrong_secret = ValidatorSecretKey::generate();
        let wrong_public = wrong_secret.public();
        let certificate = create_signed_certificate(&wrong_secret, &wrong_public);
        let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract =
            deploy_light_client(&mut db, deployer, &[address], &[1], test_admin_chain_id());

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
}
