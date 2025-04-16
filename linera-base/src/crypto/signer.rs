// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub use in_mem::InMemSigner;

use super::CryptoHash;
use crate::{
    crypto::{AccountPublicKey, AccountSignature},
    identifiers::AccountOwner,
};

/// A trait for signing keys.
#[trait_variant::make(Send + Sync)]
pub trait Signer {
    /// Creates a signature for the given `value` using the provided `owner`.
    // DEV: We sign `CryptoHash` type, rather than `&[u8]` to make sure we don't sign
    // things accidentally. See [`CryptoHash::new`] for how the type's name is included
    // in the resulting hash, providing the canonicity of the hashing process.
    fn sign(&self, owner: &AccountOwner, value: &CryptoHash) -> Option<AccountSignature>;

    /// Returns the public key corresponding to the given `owner`.
    fn get_public(&self, owner: &AccountOwner) -> Option<AccountPublicKey>;

    /// Returnes whether the given `owner` is a known signer.
    fn contains_key(&self, owner: &AccountOwner) -> bool;
}

impl Signer for Box<dyn Signer> {
    fn sign(&self, owner: &AccountOwner, value: &CryptoHash) -> Option<AccountSignature> {
        (**self).sign(owner, value)
    }

    fn get_public(&self, owner: &AccountOwner) -> Option<AccountPublicKey> {
        (**self).get_public(owner)
    }

    fn contains_key(&self, owner: &AccountOwner) -> bool {
        (**self).contains_key(owner)
    }
}

/// In-memory implementatio of the [`Signer`] trait.
mod in_mem {
    use std::{
        collections::BTreeMap,
        sync::{Arc, RwLock},
    };

    use serde::{Deserialize, Serialize};

    #[cfg(with_getrandom)]
    use crate::crypto::CryptoRng;
    use crate::{
        crypto::{AccountPublicKey, AccountSecretKey, AccountSignature, CryptoHash, Signer},
        identifiers::AccountOwner,
    };

    /// In-memory signer.
    #[derive(Clone)]
    pub struct InMemSigner(Arc<RwLock<InMemSignerInner>>);

    impl InMemSigner {
        /// Creates a new `InMemSigner` seeded with `prng_seed`.
        /// If `prng_seed` is `None`, an `OsRng` will be used.
        #[cfg(with_getrandom)]
        pub fn new(prng_seed: Option<u64>) -> Self {
            InMemSigner(Arc::new(RwLock::new(InMemSignerInner::new(prng_seed))))
        }

        /// Creates a new `InMemSigner`.
        #[cfg(not(with_getrandom))]
        pub fn new() -> Self {
            InMemSigner(Arc::new(RwLock::new(InMemSignerInner::new())))
        }

        /// Generates a new key pair from Signer's RNG. Use with care.
        #[cfg(with_getrandom)]
        pub fn generate_new(&mut self) -> AccountPublicKey {
            let mut inner = self.0.write().unwrap();
            let secret = AccountSecretKey::generate_from(&mut inner.rng_state.prng);
            inner
                .rng_state
                .keys_generated
                .checked_add(1)
                .expect("too many keys generated");
            let public = secret.public();
            let owner = AccountOwner::from(public);
            inner.keys.insert(owner, secret);
            public
        }
    }

    /// In-memory signer.
    struct InMemSignerInner {
        keys: BTreeMap<AccountOwner, AccountSecretKey>,
        #[cfg(with_getrandom)]
        rng_state: RngState,
    }

    #[cfg(with_getrandom)]
    struct RngState {
        prng: Box<dyn CryptoRng>,
        // Kept around for deterministic reconstruction of the RNG
        // across the persistence boundary.
        initial_prng_seed: Option<u64>,
        keys_generated: u64,
    }

    #[cfg(with_getrandom)]
    impl RngState {
        fn new(prng_seed: Option<u64>, keys_generated: u64) -> Self {
            let mut prng: Box<dyn CryptoRng> = prng_seed.into();
            for _ in 0..keys_generated {
                // Rebuild the PRNG state by generating dummy values.
                let _ = prng.next_u64();
            }
            RngState {
                prng,
                initial_prng_seed: prng_seed,
                keys_generated,
            }
        }
    }

    impl InMemSignerInner {
        /// Creates a new `InMemSignerInner` seeded with `prng_seed`.
        /// If `prng_seed` is `None`, an `OsRng` will be used.
        #[cfg(with_getrandom)]
        pub fn new(prng_seed: Option<u64>) -> Self {
            InMemSignerInner {
                keys: BTreeMap::new(),
                rng_state: RngState::new(prng_seed, 0),
            }
        }

        /// Creates a new `InMemSignerInner`.
        #[cfg(not(with_getrandom))]
        pub fn new() -> Self {
            InMemSignerInner {
                keys: BTreeMap::new(),
            }
        }

        fn keys(&self) -> Vec<(AccountOwner, Vec<u8>)> {
            self.keys
                .iter()
                .map(|(owner, secret)| {
                    (
                        *owner,
                        serde_json::to_vec(secret).expect("serialization should not fail"),
                    )
                })
                .collect()
        }
    }

    impl Signer for InMemSigner {
        /// Creates a signature for the given `value` using the provided `owner`.
        fn sign(&self, owner: &AccountOwner, value: &CryptoHash) -> Option<AccountSignature> {
            let inner = self.0.read().unwrap();
            let secret = inner.keys.get(owner)?;
            let signature = secret.sign_prehash(*value);
            Some(signature)
        }

        /// Returns the public key corresponding to the given `owner`.
        fn get_public(&self, owner: &AccountOwner) -> Option<AccountPublicKey> {
            let inner = self.0.read().unwrap();
            let secret = inner.keys.get(owner)?;
            Some(secret.public())
        }

        /// Returnes whether the given `owner` is a known signer.
        fn contains_key(&self, owner: &AccountOwner) -> bool {
            let inner = self.0.read().unwrap();
            inner.keys.contains_key(owner)
        }
    }

    impl FromIterator<(AccountOwner, AccountSecretKey)> for InMemSigner {
        fn from_iter<T>(input: T) -> Self
        where
            T: IntoIterator<Item = (AccountOwner, AccountSecretKey)>,
        {
            InMemSigner(Arc::new(RwLock::new(InMemSignerInner {
                keys: BTreeMap::from_iter(input),
                #[cfg(with_getrandom)]
                rng_state: RngState::new(None, 0),
            })))
        }
    }

    impl Default for InMemSignerInner {
        fn default() -> Self {
            #[cfg(with_getrandom)]
            let signer = InMemSignerInner::new(None);
            #[cfg(not(with_getrandom))]
            let signer = InMemSignerInner::new();
            signer
        }
    }

    impl Serialize for InMemSigner {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let inner = self.0.read().unwrap();
            InMemSignerInner::serialize(&*inner, serializer)
        }
    }

    impl<'de> Deserialize<'de> for InMemSigner {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let inner = InMemSignerInner::deserialize(deserializer)?;
            Ok(InMemSigner(Arc::new(RwLock::new(inner))))
        }
    }

    impl Serialize for InMemSignerInner {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            #[derive(Serialize)]
            struct Inner<'a> {
                keys: &'a Vec<(AccountOwner, Vec<u8>)>,
                #[cfg(with_getrandom)]
                prng_seed: Option<u64>,
                #[cfg(with_getrandom)]
                keys_generated: u64,
            }

            let inner = Inner {
                keys: &self.keys(),
                #[cfg(with_getrandom)]
                prng_seed: self.rng_state.initial_prng_seed,
                #[cfg(with_getrandom)]
                keys_generated: self.rng_state.keys_generated,
            };

            Inner::serialize(&inner, serializer)
        }
    }

    impl<'de> Deserialize<'de> for InMemSignerInner {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            #[derive(Deserialize)]
            struct Inner {
                keys: Vec<(AccountOwner, Vec<u8>)>,
                #[cfg(with_getrandom)]
                prng_seed: Option<u64>,
                #[cfg(with_getrandom)]
                keys_generated: u64,
            }

            let inner = Inner::deserialize(deserializer)?;

            let keys = inner
                .keys
                .into_iter()
                .map(|(owner, secret)| {
                    let secret =
                        serde_json::from_slice(&secret).map_err(serde::de::Error::custom)?;
                    Ok((owner, secret))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;

            let signer = InMemSignerInner {
                keys,
                #[cfg(with_getrandom)]
                rng_state: RngState::new(inner.prng_seed, inner.keys_generated),
            };
            Ok(signer)
        }
    }
}
