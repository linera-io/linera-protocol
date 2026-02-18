// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Gas usage measurements for LightClient and Microchain operations.

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::{CryptoHash, TestString, ValidatorSecretKey},
        data_types::{BlockHeight, Epoch},
    };
    use revm::{database::CacheDB, primitives::Address};

    use crate::{light_client::addCommitteeCall, microchain::addBlockCall, test_helpers::*};

    #[test]
    fn test_gas_light_client_add_committee() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let address = validator_evm_address(&public);

        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract =
            deploy_light_client(&mut db, deployer, &[address], &[1], test_admin_chain_id());

        let (committee_bytes, blob_hash) = create_committee_blob(&new_public);
        let transactions = create_committee_transaction(Epoch(1), blob_hash);
        let block = create_test_block(
            test_admin_chain_id(),
            Epoch::ZERO,
            BlockHeight(1),
            transactions,
        );
        let bcs_bytes = sign_and_serialize(&secret, &public, block);
        let new_uncompressed = validator_uncompressed_key(&new_public);

        let (_, _, gas_used) = call_contract(
            &mut db,
            deployer,
            contract,
            addCommitteeCall {
                data: bcs_bytes.into(),
                committeeBlob: committee_bytes.into(),
                validators: vec![new_uncompressed.into()],
            },
        );

        println!("LightClient.addCommittee gas used: {gas_used}");
    }

    #[test]
    fn test_gas_microchain_add_block() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let address = validator_evm_address(&public);

        let chain_id = CryptoHash::new(&TestString::new("test_chain"));

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let light_client =
            deploy_light_client(&mut db, deployer, &[address], &[1], test_admin_chain_id());
        let microchain = deploy_microchain(&mut db, deployer, light_client, chain_id);

        let cert = create_signed_certificate_for_chain(&secret, &public, chain_id, BlockHeight(1));
        let bcs_bytes = bcs::to_bytes(&cert).expect("BCS serialization failed");
        let (_, _, gas_used) = call_contract(
            &mut db,
            deployer,
            microchain,
            addBlockCall {
                data: bcs_bytes.into(),
            },
        );

        println!("Microchain.addBlock gas used: {gas_used}");
    }
}
