// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;
    use linera_base::{
        crypto::{
            CryptoHash, TestString, ValidatorPublicKey, ValidatorSecretKey, ValidatorSignature,
        },
        data_types::{BlockHeight, Epoch, Round},
    };
    use linera_chain::{block::ConfirmedBlock, data_types::Vote, types::ConfirmedBlockCertificate};
    use revm::{
        database::{CacheDB, EmptyDB},
        primitives::{Address, Bytes},
    };

    use crate::{
        block_proof::ProvenEvents,
        contracts::ILightClient::{
            addCommitteeCall, assertEventsCommittedCall, committeeHeightCall,
            committeeTotalWeightCall, currentEpochCall, expireEpochsBelowCall,
            minAcceptedEpochCall, registerBlockCall, registeredBlocksCall,
        },
        test_helpers::*,
    };

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
            &call,
        );

        assert_eq!(light_client.query_current_epoch(), Epoch(1));
    }

    #[test]
    fn test_light_client_committee_height() {
        let mut light_client: TestLightClient = TestLightClient::new();

        // The genesis committee (epoch 0) is set in the constructor with no
        // backing block, so its recorded admin-chain height defaults to 0.
        assert_eq!(light_client.query_committee_height(0), 0);

        // Rotate to epoch 1 via an admin block at height 7.
        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();
        let call = light_client.add_committee_call(
            &new_public,
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(7),
            test_admin_chain_id(),
        );
        call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            &call,
        );

        // The committee for epoch 1 records the admin-chain height of the block
        // that created it, so the relayer can resume scanning from there.
        assert_eq!(light_client.query_committee_height(1), 7);
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
                &call
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
                &call
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
                &call
            )
            .is_err(),
            "should reject CreateCommittee from non-admin chain"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_invalid_key_prefix() {
        use linera_base::data_types::BlobContent;

        let mut light_client: TestLightClient = TestLightClient::new();
        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        // Corrupt the blob: change the first validator key's SEC1 prefix from
        // 0x04 (uncompressed) to 0x02. The blob hash commitment is recomputed
        // over the corrupted bytes, so the failure comes from key parsing.
        let (mut committee_bytes, _) = create_committee_blob(&new_public);
        assert_eq!(committee_bytes[1], 0x04);
        committee_bytes[1] = 0x02;
        let blob_hash = CryptoHash::new(&BlobContent::new_committee(committee_bytes.clone()));

        let (proven, block_proof) = committee_call_args_for_event(
            &light_client.secret,
            &light_client.public,
            epoch_event(Epoch(1), blob_hash),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        let call =
            light_client.register_and_add_committee_call(proven, block_proof, committee_bytes);

        assert!(
            try_call_contract(
                &mut light_client.db,
                light_client.deployer,
                light_client.contract,
                &call
            )
            .is_err(),
            "should reject validator key without the uncompressed SEC1 prefix"
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
            &call_1,
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
                &call_2
            )
            .is_err(),
            "should reject committee transition from wrong epoch block"
        );
    }

    #[test]
    fn test_light_client_add_committee_rejects_user_stream_lookalike_event() {
        let mut light_client = TestLightClient::new();
        let new_secret = ValidatorSecretKey::generate();
        let new_public = new_secret.public();

        // Forge an event with the same index, stream-name bytes, and `EpochEventData` payload as
        // the real epoch event (the blob hash even matches the committee below), but emitted on a
        // user application stream rather than the system stream. Its inclusion proof checks out, so
        // the only thing that can reject the upgrade is the system-stream requirement.
        let (committee_bytes, blob_hash) = create_committee_blob(&new_public);
        let forged = forged_user_epoch_event(
            CryptoHash::new(&TestString::new("evil_app")),
            Epoch(1),
            blob_hash,
        );
        let (proven, block_proof) = committee_call_args_for_event(
            &light_client.secret,
            &light_client.public,
            forged,
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        let call =
            light_client.register_and_add_committee_call(proven, block_proof, committee_bytes);

        let Err(err) = try_call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            &call,
        ) else {
            panic!("user-stream look-alike event must not upgrade the committee");
        };
        // Confirm it was the system-stream requirement that rejected it, not an incidental check:
        // the revert reason is ABI-encoded in the output.
        let reason_hex = hex::encode("not a system event");
        assert!(
            err.contains(&reason_hex),
            "expected 'not a system event' revert, got: {err}"
        );
        assert_eq!(light_client.query_current_epoch(), Epoch::ZERO);
    }

    #[test]
    fn test_light_client_verify_block() {
        let mut light_client: TestLightClient = TestLightClient::new();

        let certificate = create_signed_certificate(&light_client.secret, &light_client.public);
        let bcs_bytes = bcs::to_bytes(&crate::block_proof::BlockProof::from_certificate(
            &certificate,
        ))
        .expect("BCS serialization failed");

        light_client.verify_block(bcs_bytes);
    }

    #[test]
    fn test_light_client_verify_first_round_block() {
        let mut light_client: TestLightClient = TestLightClient::new();

        // A confirmation attested as being in the chain's first round, so its C-votes sign the
        // first-round flag. The proof carries the flag and the light client must reproduce it in
        // the signed `VoteValue`, or `ecrecover` would recover the wrong signer.
        let certificate =
            create_signed_certificate_first_round(&light_client.secret, &light_client.public);
        let bcs_bytes = bcs::to_bytes(&crate::block_proof::BlockProof::from_certificate(
            &certificate,
        ))
        .expect("BCS serialization failed");

        light_client.verify_block(bcs_bytes);
    }

    #[test]
    fn test_light_client_register_block_records_events_hash() {
        let mut light_client = TestLightClient::new();
        let certificate = create_signed_certificate(&light_client.secret, &light_client.public);
        let proof_bytes = bcs::to_bytes(&crate::block_proof::BlockProof::from_certificate(
            &certificate,
        ))
        .expect("BCS serialization failed");

        let (block_hash, _, _) = call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            &registerBlockCall {
                blockProof: proof_bytes.into(),
            },
        );

        // `registerBlock` returns the block hash, which is `hash(header)`.
        assert_eq!(block_hash.0, *certificate.hash().as_bytes());

        // The stored metadata matches the header: events hash, height, and chain id.
        let (block_meta, _, _) = call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            &registeredBlocksCall {
                blockHash: block_hash,
            },
        );
        let header = &certificate.block().header;
        assert_eq!(block_meta.eventsHash.0, *header.events_hash.as_bytes());
        assert_eq!(block_meta.height, header.height.0);
        assert_eq!(block_meta.chainId.0, *header.chain_id.0.as_bytes());
    }

    #[test]
    fn test_light_client_prove_events_committed_against_header() {
        use alloy_primitives::{Bytes, B256};
        use linera_base::{
            data_types::Event,
            identifiers::{GenericApplicationId, StreamId, StreamName},
        };

        use crate::block_proof::EventInclusionProof;

        let mut lc = TestLightClient::new();
        let chain = CryptoHash::new(&TestString::new("test_chain"));
        let make_event = |value: &[u8], index| Event {
            stream_id: StreamId {
                application_id: GenericApplicationId::System,
                stream_name: StreamName(b"burns".to_vec()),
            },
            index,
            value: value.to_vec(),
        };
        // Three transactions carrying 1, 2, and 1 events.
        let events = vec![
            vec![make_event(b"a", 0)],
            vec![make_event(b"b", 1), make_event(b"c", 2)],
            vec![make_event(b"d", 3)],
        ];
        let certificate = create_signed_certificate_with_events(
            &lc.secret,
            &lc.public,
            chain,
            BlockHeight(1),
            events.clone(),
        );
        // The events commitment is taken straight from the signed header — no `registerBlock`.
        let events_hash = B256::from(*certificate.block().header.events_hash.as_bytes());

        let to_b256 = |h: &CryptoHash| B256::from(*h.as_bytes());
        let tx_index = 1usize;
        let positions = [1u32];
        let proof = EventInclusionProof::new(&events, tx_index, &positions);
        let siblings: Vec<B256> = proof.siblings().iter().map(to_b256).collect();
        let make_call = |event_bcs: Vec<Bytes>| assertEventsCommittedCall {
            eventsHash: events_hash,
            eventBcs: event_bcs,
            txIndex: proof.tx_index,
            numTxs: proof.num_txs,
            numEventsInTx: proof.num_events_in_tx,
            positions: positions.to_vec(),
            siblings: siblings.clone(),
        };

        let good_bcs: Vec<Bytes> = positions
            .iter()
            .map(|&p| {
                bcs::to_bytes(&events[tx_index][p as usize])
                    .expect("BCS serialization failed")
                    .into()
            })
            .collect();

        // The event the header commits to folds back to its events_hash.
        call_contract(
            &mut lc.db,
            lc.deployer,
            lc.contract,
            &make_call(good_bcs.clone()),
        );

        // A tampered event does not.
        let bad: Vec<Bytes> = vec![bcs::to_bytes(&make_event(b"x", 9))
            .expect("BCS serialization failed")
            .into()];
        assert!(
            try_call_contract(&mut lc.db, lc.deployer, lc.contract, &make_call(bad)).is_err(),
            "tampered event must not fold to the header's events_hash"
        );

        // An unrelated events_hash is rejected.
        let mut wrong_hash = make_call(good_bcs.clone());
        wrong_hash.eventsHash = B256::ZERO;
        assert!(
            try_call_contract(&mut lc.db, lc.deployer, lc.contract, &wrong_hash).is_err(),
            "events must not fold to an unrelated events_hash"
        );

        // A txIndex past the block's transaction count is rejected.
        let mut bad_tx = make_call(good_bcs);
        bad_tx.txIndex = proof.num_txs;
        assert!(
            try_call_contract(&mut lc.db, lc.deployer, lc.contract, &bad_tx).is_err(),
            "out-of-range txIndex must be rejected"
        );
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
        let bcs_bytes = bcs::to_bytes(&crate::block_proof::BlockProof::from_certificate(
            &certificate,
        ))
        .expect("BCS serialization failed");

        assert!(
            try_call_contract(
                &mut db,
                deployer,
                contract,
                &registerBlockCall {
                    blockProof: bcs_bytes.into(),
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
            &call_1,
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
        let bcs_bytes = bcs::to_bytes(&crate::block_proof::BlockProof::from_certificate(
            &certificate,
        ))
        .expect("BCS serialization failed");

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
        let bcs_bytes = bcs::to_bytes(&crate::block_proof::BlockProof::from_certificate(
            &certificate,
        ))
        .expect("BCS serialization failed");

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

        let mut bcs_bytes = bcs::to_bytes(&crate::block_proof::BlockProof::from_certificate(
            &certificate,
        ))
        .expect("BCS serialization failed");

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

    /// A retired committee can still sign a verifiable block proof until its
    /// epoch is expired via `expireEpochsBelow` (weak-subjectivity floor).
    #[test]
    fn test_light_client_expire_epochs_below() {
        let mut light_client = TestLightClient::new();

        // Baseline: a valid epoch-0 block proof verifies, nothing expired yet.
        let block_0 = create_test_block(
            CryptoHash::new(&TestString::new("test_chain")),
            Epoch::ZERO,
            BlockHeight(1),
            vec![],
        );
        let proof_0 = sign_and_serialize(&light_client.secret, &light_client.public, block_0);
        light_client.verify_block(proof_0.clone());

        // Rotate to epoch 1.
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
            &call_1,
        );
        assert_eq!(light_client.query_current_epoch(), Epoch(1));

        // Weak subjectivity: the epoch-0 proof still verifies after rotation.
        light_client.verify_block(proof_0.clone());

        // The epoch-0 committee is present in storage before expiry.
        let (weight_before, _, _) = call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            &committeeTotalWeightCall { epoch: 0 },
        );
        assert!(
            weight_before > 0,
            "epoch 0 committee should exist before expiry"
        );

        // Retire epoch 0.
        call_contract(
            &mut light_client.db,
            test_proposer(),
            light_client.contract,
            &expireEpochsBelowCall { newMinEpoch: 1 },
        );
        let (min_epoch, _, _) = call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            &minAcceptedEpochCall {},
        );
        assert_eq!(min_epoch, 1, "minAcceptedEpoch should be raised to 1");

        // The epoch-0 committee storage is cleared, not merely floored out.
        let (weight_after, _, _) = call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            &committeeTotalWeightCall { epoch: 0 },
        );
        assert_eq!(
            weight_after, 0,
            "epoch 0 committee storage should be cleared after expiry"
        );

        // The retired epoch-0 proof is now rejected as expired.
        assert!(
            light_client.try_verify_block(proof_0).is_err(),
            "block proof from a retired epoch must be rejected"
        );

        // The current committee (epoch 1) still verifies.
        let block_1 = create_test_block(
            CryptoHash::new(&TestString::new("test_chain")),
            Epoch(1),
            BlockHeight(2),
            vec![],
        );
        let proof_1 = sign_and_serialize(&secret_1, &public_1, block_1);
        light_client.verify_block(proof_1);
    }

    /// `expireEpochsBelow` is monotonic and can never retire the current epoch.
    #[test]
    fn test_light_client_expire_epochs_below_invariants() {
        let mut light_client = TestLightClient::new();

        // At epoch 0 nothing can be expired: newMinEpoch must exceed
        // minAcceptedEpoch (0) yet not exceed currentEpoch (0).
        assert!(
            try_call_contract(
                &mut light_client.db,
                test_proposer(),
                light_client.contract,
                &expireEpochsBelowCall { newMinEpoch: 1 },
            )
            .is_err(),
            "cannot expire at epoch 0"
        );

        // Rotate to epoch 1.
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
            &call_1,
        );

        // The current epoch can never be retired: newMinEpoch may not exceed
        // currentEpoch.
        assert!(
            try_call_contract(
                &mut light_client.db,
                test_proposer(),
                light_client.contract,
                &expireEpochsBelowCall { newMinEpoch: 2 },
            )
            .is_err(),
            "cannot expire the current epoch"
        );

        // Retire epoch 0 (floor -> 1) while still at epoch 1.
        call_contract(
            &mut light_client.db,
            test_proposer(),
            light_client.contract,
            &expireEpochsBelowCall { newMinEpoch: 1 },
        );

        // Monotonic: cannot repeat or decrease the floor.
        assert!(
            try_call_contract(
                &mut light_client.db,
                test_proposer(),
                light_client.contract,
                &expireEpochsBelowCall { newMinEpoch: 1 },
            )
            .is_err(),
            "minAcceptedEpoch must strictly increase"
        );
        assert!(
            try_call_contract(
                &mut light_client.db,
                test_proposer(),
                light_client.contract,
                &expireEpochsBelowCall { newMinEpoch: 0 },
            )
            .is_err(),
            "minAcceptedEpoch cannot decrease"
        );
    }

    /// Common test state for LightClient tests with a single initial validator.
    struct TestLightClient {
        db: CacheDB<EmptyDB>,
        deployer: Address,
        secret: ValidatorSecretKey,
        public: ValidatorPublicKey,
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
            &mut self,
            new_public: &ValidatorPublicKey,
            new_epoch: Epoch,
            block_epoch: Epoch,
            height: BlockHeight,
            chain_id: CryptoHash,
        ) -> addCommitteeCall {
            let (committee_bytes, blob_hash) = create_committee_blob(new_public);
            let (proven, block_proof) = committee_call_args_for_event(
                &self.secret,
                &self.public,
                epoch_event(new_epoch, blob_hash),
                block_epoch,
                height,
                chain_id,
            );
            self.register_and_add_committee_call(proven, block_proof, committee_bytes)
        }

        /// Registers the admin block (the on-chain quorum check), then builds the `addCommittee`
        /// call referencing it by hash — the register-then-prove flow `processBurns` also uses.
        fn register_and_add_committee_call(
            &mut self,
            proven: ProvenEvents,
            block_proof: Bytes,
            committee_blob: Vec<u8>,
        ) -> addCommitteeCall {
            self.verify_block(block_proof.to_vec());
            build_add_committee_call(proven, committee_blob)
        }

        fn query_current_epoch(&mut self) -> Epoch {
            let (epoch, _, _) = call_contract(
                &mut self.db,
                self.deployer,
                self.contract,
                &currentEpochCall {},
            );
            Epoch(epoch)
        }

        fn query_committee_height(&mut self, epoch: u32) -> u64 {
            let (height, _, _) = call_contract(
                &mut self.db,
                self.deployer,
                self.contract,
                &committeeHeightCall { epoch },
            );
            height
        }

        /// Registers a block, which runs the same quorum verification the old `verifyBlock` did.
        fn verify_block(&mut self, proof_bytes: Vec<u8>) {
            call_contract(
                &mut self.db,
                self.deployer,
                self.contract,
                &registerBlockCall {
                    blockProof: proof_bytes.into(),
                },
            );
        }

        fn try_verify_block(&mut self, proof_bytes: Vec<u8>) -> Result<(), String> {
            try_call_contract(
                &mut self.db,
                self.deployer,
                self.contract,
                &registerBlockCall {
                    blockProof: proof_bytes.into(),
                },
            )
            .map(|_| ())
        }
    }

    #[test]
    fn test_light_client_add_committee_two_validators() {
        let mut light_client = TestLightClient::new();
        let new_secret1 = ValidatorSecretKey::generate();
        let new_public1 = new_secret1.public();
        let new_secret2 = ValidatorSecretKey::generate();
        let new_public2 = new_secret2.public();

        let (proven, block_proof, committee_bytes) = create_add_committee_call_multi(
            &light_client.secret,
            &light_client.public,
            &[new_public1, new_public2],
            Epoch(1),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        let call =
            light_client.register_and_add_committee_call(proven, block_proof, committee_bytes);
        call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            &call,
        );

        assert_eq!(light_client.query_current_epoch(), Epoch(1));
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
            linera_execution::Committee::new(validators, ResourceControlPolicy::default())
                .expect("committee creation failed");
        let committee_bytes = bcs::to_bytes(&committee).expect("committee serialization failed");
        let blob_content = BlobContent::new_committee(committee_bytes.clone());
        let blob_hash = CryptoHash::new(&blob_content);

        let (proven, block_proof) = committee_call_args_for_event(
            &light_client.secret,
            &light_client.public,
            epoch_event(Epoch(1), blob_hash),
            Epoch::ZERO,
            BlockHeight(1),
            test_admin_chain_id(),
        );
        let call =
            light_client.register_and_add_committee_call(proven, block_proof, committee_bytes);
        call_contract(
            &mut light_client.db,
            light_client.deployer,
            light_client.contract,
            &call,
        );

        assert_eq!(light_client.query_current_epoch(), Epoch(1));
    }

    /// Creates a committee blob with multiple validators and returns `(committee_bytes, blob_hash)`.
    fn create_multi_committee_blob(publics: &[ValidatorPublicKey]) -> (Vec<u8>, CryptoHash) {
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
            linera_execution::Committee::new(validators, ResourceControlPolicy::default())
                .expect("committee creation failed");
        let bytes = bcs::to_bytes(&committee).expect("committee serialization failed");
        let blob_content = BlobContent::new_committee(bytes.clone());
        let blob_hash = CryptoHash::new(&blob_content);
        (bytes, blob_hash)
    }

    /// Builds the register-then-`addCommittee` inputs for a multi-validator transition. The block
    /// is signed by `signer_secret`/`signer_public`. Returns the proven-events witness, the BCS
    /// block proof (to `registerBlock` first), and the committee blob.
    fn create_add_committee_call_multi(
        signer_secret: &ValidatorSecretKey,
        signer_public: &ValidatorPublicKey,
        new_publics: &[ValidatorPublicKey],
        new_epoch: Epoch,
        block_epoch: Epoch,
        height: BlockHeight,
        chain_id: CryptoHash,
    ) -> (ProvenEvents, Bytes, Vec<u8>) {
        let (committee_bytes, blob_hash) = create_multi_committee_blob(new_publics);
        let (proven, block_proof) = committee_call_args_for_event(
            signer_secret,
            signer_public,
            epoch_event(new_epoch, blob_hash),
            block_epoch,
            height,
            chain_id,
        );

        (proven, block_proof, committee_bytes)
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

                // Create two-validator committee, register the admin block, then call addCommittee.
                let (proven, block_proof, committee_bytes) = create_add_committee_call_multi(
                    &signer_secret,
                    &signer_public,
                    &[public1, public2],
                    Epoch(1),
                    Epoch::ZERO,
                    BlockHeight(1),
                    test_admin_chain_id(),
                );
                call_contract(
                    &mut db,
                    deployer,
                    contract,
                    &registerBlockCall { blockProof: block_proof },
                );
                let call = build_add_committee_call(proven, committee_bytes);

                let result = try_call_contract(&mut db, deployer, contract, &call);
                prop_assert!(result.is_ok(), "addCommittee failed: {:?}", result.err());
            }
        }
    }
}
