// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloy_sol_types::sol;

/// Solidity source for the LightClient contract.
pub const SOURCE: &str = include_str!("solidity/LightClient.sol");

sol! {
    function addCommittee(
        bytes calldata data,
        bytes calldata committeeBlob,
        address[] calldata validators,
        uint64[] calldata weights
    ) external;

    function verifyBlock(bytes calldata data) external view;

    function currentEpoch() external view returns (uint32);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use alloy_sol_types::SolCall;
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

    use super::{addCommitteeCall, verifyBlockCall};
    use crate::test_helpers::*;

    #[test]
    fn test_light_client_add_committee() {
        // Current validator (epoch 0)
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let address = validator_evm_address(&public);

        // New validator (epoch 1)
        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();
        let new_address = validator_evm_address(&new_public);

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
            CryptoHash::new(&TestString::new("test_chain")),
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
        let contract = deploy_light_client(&mut db, deployer, &[address], &[1]);

        let calldata = addCommitteeCall {
            data: bcs_bytes.into(),
            committeeBlob: committee_bytes.into(),
            validators: vec![new_address],
            weights: vec![1],
        }
        .abi_encode();
        call_contract(&mut db, deployer, contract, calldata);
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
        let new_address = validator_evm_address(&new_public);

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
            CryptoHash::new(&TestString::new("test_chain")),
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
        let contract = deploy_light_client(&mut db, deployer, &[address], &[1]);

        // Pass a different (wrong) committee blob
        let wrong_blob = vec![0x01, 0x02, 0x03];
        let calldata = addCommitteeCall {
            data: bcs_bytes.into(),
            committeeBlob: wrong_blob.into(),
            validators: vec![new_address],
            weights: vec![1],
        }
        .abi_encode();
        assert!(
            try_call_contract(&mut db, deployer, contract, calldata).is_err(),
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
        let new_address = validator_evm_address(&new_public);

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
            CryptoHash::new(&TestString::new("test_chain")),
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
        let contract = deploy_light_client(&mut db, deployer, &[address], &[1]);

        let calldata = addCommitteeCall {
            data: bcs_bytes.into(),
            committeeBlob: committee_bytes.into(),
            validators: vec![new_address],
            weights: vec![1],
        }
        .abi_encode();
        assert!(
            try_call_contract(&mut db, deployer, contract, calldata).is_err(),
            "should reject non-sequential epoch"
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
        let contract = deploy_light_client(&mut db, deployer, &[address], &[1]);

        let calldata = verifyBlockCall {
            data: bcs_bytes.into(),
        }
        .abi_encode();
        call_contract(&mut db, deployer, contract, calldata);
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
        let contract = deploy_light_client(&mut db, deployer, &[address], &[1]);

        let calldata = verifyBlockCall {
            data: bcs_bytes.into(),
        }
        .abi_encode();
        assert!(
            try_call_contract(&mut db, deployer, contract, calldata).is_err(),
            "should reject certificate signed by unknown validator"
        );
    }
}
