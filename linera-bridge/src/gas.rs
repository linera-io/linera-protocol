// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Gas usage measurements for LightClient and Microchain operations.

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::ValidatorSecretKey,
        data_types::{BlockHeight, Epoch},
    };
    use revm::{database::CacheDB, primitives::Address};

    use crate::{contracts::ILightClient::registerBlockCall, test_helpers::*};

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
        let (proven, block_proof) = committee_block_args(
            &secret,
            &public,
            Epoch(1),
            blob_hash,
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        // Register the admin block first, then prove the committee event against it (the
        // register-then-prove flow `addCommittee` now shares with `processBurns`).
        call_contract(
            &mut db,
            deployer,
            contract,
            &registerBlockCall {
                blockProof: block_proof,
            },
        );
        let call = build_add_committee_call(proven, committee_bytes);

        let (_, _, gas_used) = call_contract(&mut db, deployer, contract, &call);

        println!("LightClient.addCommittee gas used: {gas_used}");
    }
}
