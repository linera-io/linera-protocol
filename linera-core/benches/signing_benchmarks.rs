// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use criterion::criterion_main;

mod secp256k1 {
    use criterion::{criterion_group, Criterion};
    use k256::ecdsa::{
        signature::{Signer, Verifier},
        Signature, SigningKey,
    };
    use rand_core::OsRng;

    fn keypair_generation(c: &mut Criterion) {
        c.bench_function("secp256k1 keypair generation", |b| {
            b.iter(|| {
                let _sk = SigningKey::random(&mut OsRng);
            });
        });
    }

    fn sign(c: &mut Criterion) {
        let signing_key: SigningKey = SigningKey::random(&mut OsRng); // Serialize with `::to_bytes()`
        let message = b"";

        // Note: The signature type must be annotated or otherwise inferable as
        // `Signer` has many impls of the `Signer` trait (for both regular and
        // recoverable signature types).
        c.bench_function("secp256k1 signing", |b| {
            b.iter(|| {
                let _s: Signature = signing_key.sign(message);
            })
        });
    }

    fn verify(c: &mut Criterion) {
        let signing_key: SigningKey = SigningKey::random(&mut OsRng); // Serialize with `::to_bytes()`
        let message = b"";

        let sig: Signature = signing_key.sign(message);
        let vk = signing_key.verifying_key();

        c.bench_function("secp256k1 verification", |b| {
            b.iter(|| {
                let _ = vk.verify(message, &sig);
            })
        });
    }

    fn verify_batch(c: &mut Criterion) {
        // There's no native way of verifying a batch of secp256k1 but there is one for ed25519
        // and we want to learn what are the differences in performance.

        static BATCH_SIZES: [usize; 6] = [4, 8, 16, 32, 64, 96];

        let mut group = c.benchmark_group("secp256k1 batch verification");
        for size in BATCH_SIZES {
            let name = format!("size={}", size);
            group.bench_function(name, |b| {
                let signing_keys: Vec<SigningKey> =
                    (0..size).map(|_| SigningKey::random(&mut OsRng)).collect(); // Serialize with `::to_bytes()`

                let verifying_keys: Vec<_> = signing_keys
                    .iter()
                    .map(|sk| sk.verifying_key().clone())
                    .collect();

                let msg: &[u8] = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
                let signatures: Vec<Signature> =
                    signing_keys.iter().map(|key| key.sign(msg)).collect();

                b.iter(move || {
                    for i in 0..size {
                        let _ = verifying_keys[i].verify(msg, &signatures[i]);
                    }
                });
            });
        }
    }

    criterion_group! {
        name = secp256k1_benches;
        config = Criterion::default();
        targets =
            sign,
            keypair_generation,
            verify,
            verify_batch
    }
}

criterion_main!(secp256k1::secp256k1_benches);
