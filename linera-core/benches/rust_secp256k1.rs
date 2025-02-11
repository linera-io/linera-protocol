// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use criterion::criterion_main;

mod rust_secp256k1 {
    use criterion::{criterion_group, Criterion};
    use rand_core::OsRng;
    use secp256k1::{ecdsa::Signature, Message, SecretKey};

    fn keypair_generation(c: &mut Criterion) {
        let secp = secp256k1::Secp256k1::new();
        c.bench_function("rust_secp256k1 keypair generation", |b| {
            b.iter(|| {
                let (_secret_key, _public_key) = secp.generate_keypair(&mut OsRng);
            });
        });
    }

    fn sign(c: &mut Criterion) {
        let secp = secp256k1::Secp256k1::new();
        let (secret_key, _public_key) = secp.generate_keypair(&mut OsRng);
        let message = Message::from_digest([
            0xaa, 0xdf, 0x7d, 0xe7, 0x82, 0x03, 0x4f, 0xbe, 0x3d, 0x3d, 0xb2, 0xcb, 0x13, 0xc0,
            0xcd, 0x91, 0xbf, 0x41, 0xcb, 0x08, 0xfa, 0xc7, 0xbd, 0x61, 0xd5, 0x44, 0x53, 0xcf,
            0x6e, 0x82, 0xb4, 0x50,
        ]);

        // Note: The signature type must be annotated or otherwise inferable as
        // `Signer` has many impls of the `Signer` trait (for both regular and
        // recoverable signature types).
        c.bench_function("rust_secp256k1 signing", |b| {
            b.iter(|| {
                let _s: Signature = secp.sign_ecdsa(&message, &secret_key);
            })
        });
    }

    fn verify(c: &mut Criterion) {
        let secp = secp256k1::Secp256k1::new();
        let (secret_key, public_key) = secp.generate_keypair(&mut OsRng);
        let message = Message::from_digest([
            0xaa, 0xdf, 0x7d, 0xe7, 0x82, 0x03, 0x4f, 0xbe, 0x3d, 0x3d, 0xb2, 0xcb, 0x13, 0xc0,
            0xcd, 0x91, 0xbf, 0x41, 0xcb, 0x08, 0xfa, 0xc7, 0xbd, 0x61, 0xd5, 0x44, 0x53, 0xcf,
            0x6e, 0x82, 0xb4, 0x50,
        ]);

        let sig: Signature = secp.sign_ecdsa(&message, &secret_key);

        c.bench_function("rust_secp256k1 verification", |b| {
            b.iter(|| {
                let _ = secp.verify_ecdsa(&message, &sig, &public_key);
            })
        });
    }

    fn verify_batch(c: &mut Criterion) {
        // There's no native way of verifying a batch of secp256k1 but there is one for ed25519
        // and we want to learn what are the differences in performance.

        static BATCH_SIZES: [usize; 6] = [4, 8, 16, 32, 64, 96];

        let mut group = c.benchmark_group("rust_secp256k1 batch verification");
        for size in BATCH_SIZES {
            let name = format!("size={}", size);
            group.bench_function(name, |b| {
                let secp = secp256k1::Secp256k1::new();

                let signing_keys: Vec<SecretKey> = (0..size)
                    .map(|_| secp256k1::SecretKey::new(&mut OsRng))
                    .collect(); // Serialize with `::to_bytes()`

                let verifying_keys: Vec<_> = signing_keys
                    .iter()
                    .map(|sk| secp256k1::PublicKey::from_secret_key(&secp, &sk))
                    .collect();

                let message = Message::from_digest([
                    0xaa, 0xdf, 0x7d, 0xe7, 0x82, 0x03, 0x4f, 0xbe, 0x3d, 0x3d, 0xb2, 0xcb, 0x13,
                    0xc0, 0xcd, 0x91, 0xbf, 0x41, 0xcb, 0x08, 0xfa, 0xc7, 0xbd, 0x61, 0xd5, 0x44,
                    0x53, 0xcf, 0x6e, 0x82, 0xb4, 0x50,
                ]);
                let signatures: Vec<Signature> = signing_keys
                    .iter()
                    .map(|key| secp.sign_ecdsa(&message, &key))
                    .collect();

                b.iter(move || {
                    for i in 0..size {
                        let _ = secp.verify_ecdsa(&message, &signatures[i], &verifying_keys[i]);
                    }
                });
            });
        }
    }

    criterion_group! {
        name = rust_secp256k1_benches;
        config = Criterion::default();
        targets =
            sign,
            keypair_generation,
            verify,
            verify_batch
    }
}

criterion_main!(rust_secp256k1::rust_secp256k1_benches);
