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

    use crate::{contracts::IMicrochain::addBlockCall, test_helpers::*};

    #[test]
    fn test_gas_light_client_add_committee() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let addr = validator_evm_address(&public);

        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let contract =
            deploy_light_client(&mut db, deployer, &[addr], &[1], test_admin_chain_id(), 0);

        let (committee_bytes, blob_hash) = create_committee_blob(&new_public);
        let args = committee_block_args(
            &secret,
            &public,
            Epoch(1),
            blob_hash,
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        let new_uncompressed = validator_uncompressed_key(&new_public);
        let call = build_add_committee_call(args, committee_bytes, vec![new_uncompressed.into()]);

        let (_, _, gas_used) = call_contract(&mut db, deployer, contract, &call);

        println!("LightClient.addCommittee gas used: {gas_used}");
    }

    #[test]
    fn test_gas_microchain_add_block() {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let addr = validator_evm_address(&public);

        let chain_id = CryptoHash::new(&TestString::new("test_chain"));

        let deployer = Address::ZERO;
        let mut db = CacheDB::default();
        let light_client =
            deploy_light_client(&mut db, deployer, &[addr], &[1], test_admin_chain_id(), 0);
        let microchain = deploy_microchain(&mut db, deployer, light_client, chain_id);

        let cert = create_signed_certificate_for_chain(&secret, &public, chain_id, BlockHeight(1));
        let (block_proof, event_bcs, events_per_tx) = add_block_args(&cert);
        let (_, _, gas_used) = call_contract(
            &mut db,
            deployer,
            microchain,
            &addBlockCall {
                blockProof: block_proof,
                eventBcs: event_bcs,
                eventsPerTx: events_per_tx,
            },
        );

        println!("Microchain.addBlock gas used: {gas_used}");
    }
}
