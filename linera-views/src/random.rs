// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rand::{Rng, SeedableRng};

// The following seed is chosen to have equal numbers of 1s and 0s, as advised by
// https://docs.rs/rand/latest/rand/rngs/struct.SmallRng.html
// Specifically, it's "01" × 32 in binary
const RNG_SEED: u64 = 6148914691236517205;

/// A deterministic RNG.
pub type DeterministicRng = rand::rngs::SmallRng;

/// A RNG that is non-deterministic if the platform supports it.
pub struct NonDeterministicRng(
    #[cfg(target_arch = "wasm32")] std::sync::MutexGuard<'static, DeterministicRng>,
    #[cfg(not(target_arch = "wasm32"))] rand::rngs::ThreadRng,
);

impl NonDeterministicRng {
    /// Access the internal RNG.
    pub fn rng_mut(&mut self) -> &mut impl Rng {
        #[cfg(target_arch = "wasm32")]
        {
            &mut *self.0
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            &mut self.0
        }
    }
}

/// Returns a deterministic RNG for testing.
pub fn make_deterministic_rng() -> DeterministicRng {
    rand::rngs::SmallRng::seed_from_u64(RNG_SEED)
}

/// Returns a non-deterministic RNG where supported.
pub fn make_nondeterministic_rng() -> NonDeterministicRng {
    #[cfg(target_arch = "wasm32")]
    {
        use std::sync::{Mutex, OnceLock};

        use rand::rngs::SmallRng;

        static RNG: OnceLock<Mutex<SmallRng>> = OnceLock::new();
        NonDeterministicRng(
            RNG.get_or_init(|| Mutex::new(make_deterministic_rng()))
                .lock()
                .expect("failed to lock RNG mutex"),
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        NonDeterministicRng(rand::thread_rng())
    }
}

/// Get a random alphanumeric string that can be used for all tests.
pub fn generate_random_alphanumeric_string(length: usize) -> String {
    // Define the characters that are allowed in the alphanumeric string
    let charset: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";

    let alphanumeric_string: String = (0..length)
        .map(|_| {
            let random_index = make_nondeterministic_rng()
                .rng_mut()
                .gen_range(0..charset.len());
            charset[random_index] as char
        })
        .collect();

    alphanumeric_string
}

/// Returns a unique namespace for testing.
pub fn generate_test_namespace() -> String {
    let entry = generate_random_alphanumeric_string(20);
    let namespace = format!("table_{}", entry);
    tracing::warn!("Generating namespace={}", namespace);
    namespace
}
