// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{
    linera_base_types::{
        ApplicationId, BcsHashable, BlockHeight, ChainId, CryptoHash, WithContractAbi,
    },
    views::{RootView, View},
    Contract, ContractRuntime,
};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use random_source::RandomSourceAbi;
use serde::{Deserialize, Serialize};

use self::state::RandomSourceState;

pub struct RandomSourceContract {
    state: RandomSourceState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(RandomSourceContract);

impl WithContractAbi for RandomSourceContract {
    type Abi = RandomSourceAbi;
}

/// Inputs combined into the seed for the deterministic RNG. Only the
/// `transaction_index` makes the seed unique across consecutive operations
/// within the same block; the other fields make it unique across applications,
/// chains, and blocks.
#[derive(Serialize, Deserialize)]
struct RandomSourceSeed {
    chain_id: ChainId,
    application_id: ApplicationId,
    block_height: BlockHeight,
    transaction_index: u32,
}

impl BcsHashable<'_> for RandomSourceSeed {}

impl Contract for RandomSourceContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = RandomSourceState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        RandomSourceContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {}

    async fn execute_operation(&mut self, _operation: ()) {
        // Asserting that the operation runs as the first transaction in the
        // block exercises the fix for issue #2411: previously, the operation
        // would have been preceded by an implicit system transaction.
        let transaction_index = self.runtime.transaction_index();
        assert_eq!(
            transaction_index, 0,
            "Expected operation to be the first transaction in the block, \
             got transaction index {transaction_index}",
        );

        let seed_input = RandomSourceSeed {
            chain_id: self.runtime.chain_id(),
            application_id: self.runtime.application_id().forget_abi(),
            block_height: self.runtime.block_height(),
            transaction_index,
        };
        let hash_bytes = CryptoHash::new(&seed_input).as_bytes().0;
        let seed = u64::from_le_bytes([
            hash_bytes[0],
            hash_bytes[1],
            hash_bytes[2],
            hash_bytes[3],
            hash_bytes[4],
            hash_bytes[5],
            hash_bytes[6],
            hash_bytes[7],
        ]);

        let mut rng = StdRng::seed_from_u64(seed);
        let sample1 = rng.next_u64();
        let sample2 = rng.next_u64();
        assert_ne!(
            sample1, sample2,
            "Two consecutive samples from the same RNG must differ",
        );

        self.state.seed.set(seed);
        self.state.sample1.set(sample1);
        self.state.sample2.set(sample2);
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("RandomSource application doesn't support cross-chain messages");
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}
