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
    /// Generates a new signing key for the Self type.
    /// New secret key is inserted into Signer's memory and the `AccountPublicKey` is returned.
    #[cfg(with_getrandom)]
    fn generate_new(&mut self) -> AccountPublicKey;

    /// Creates a signature for the given `value` using the provided `owner`.
    // DEV: We sign `CryptoHash` type, rather than `&[u8]` to make sure we don't sign
    // things accidentally. See [`CryptoHash::new`] for how the type's name is included
    // in the resulting hash, providing the canonicity of the hashing process.
    fn sign(&self, owner: &AccountOwner, value: &CryptoHash) -> Option<AccountSignature>;

    /// Returns the public key corresponding to the given `owner`.
    fn get_public(&self, owner: &AccountOwner) -> Option<AccountPublicKey>;

    /// Returnes whether the given `owner` is a known signer.
    fn contains_key(&self, owner: &AccountOwner) -> bool;

    /// Returns a clone of the `Signer` as a boxed trait object.
    fn clone_box(&self) -> Box<dyn Signer>;
}

impl Clone for Box<dyn Signer> {
    fn clone(&self) -> Box<dyn Signer> {
        self.clone_box()
    }
}

impl Signer for Box<dyn Signer> {
    #[cfg(with_getrandom)]
    fn generate_new(&mut self) -> AccountPublicKey {
        (**self).generate_new()
    }

    fn sign(&self, owner: &AccountOwner, value: &CryptoHash) -> Option<AccountSignature> {
        (**self).sign(owner, value)
    }

    fn get_public(&self, owner: &AccountOwner) -> Option<AccountPublicKey> {
        (**self).get_public(owner)
    }

    fn contains_key(&self, owner: &AccountOwner) -> bool {
        (**self).contains_key(owner)
    }

    fn clone_box(&self) -> Box<dyn Signer> {
        (**self).clone_box()
    }
}

/// In-memory implementatio of the [`Signer`] trait.
mod in_mem {
    #[cfg(with_getrandom)]
    use std::sync::Mutex;
    use std::{collections::BTreeMap, sync::Arc};

    use serde::{Deserialize, Serialize};

    #[cfg(with_getrandom)]
    use crate::crypto::CryptoRng;
    use crate::{
        crypto::{AccountPublicKey, AccountSecretKey, AccountSignature, CryptoHash, Signer},
        identifiers::AccountOwner,
    };

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

    #[cfg(with_getrandom)]
    impl Clone for RngState {
        fn clone(&self) -> Self {
            RngState::new(self.initial_prng_seed, self.keys_generated)
        }
    }

    /// In-memory signer.
    pub struct InMemSigner {
        keys: Arc<BTreeMap<AccountOwner, AccountSecretKey>>,
        #[cfg(with_getrandom)]
        rng_state: Arc<Mutex<RngState>>,
    }

    impl InMemSigner {
        /// Creates a new `InMemSigner` seeded with `prng_seed`.
        /// If `prng_seed` is `None`, an `OsRng` will be used.
        #[cfg(with_getrandom)]
        pub fn new(prng_seed: Option<u64>) -> Self {
            let rng_state = RngState::new(prng_seed, 0);
            InMemSigner {
                keys: Arc::new(BTreeMap::new()),
                rng_state: Arc::new(Mutex::new(rng_state)),
            }
        }

        /// Creates a new `InMemSigner`.
        #[cfg(not(with_getrandom))]
        pub fn new() -> Self {
            InMemSigner {
                keys: Arc::new(BTreeMap::new()),
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
        /// Generates a new key pair from Signer's RNG. Use with care.
        #[cfg(with_getrandom)]
        fn generate_new(&mut self) -> AccountPublicKey {
            let mut rng_state = self.rng_state.lock().unwrap();
            let secret = AccountSecretKey::generate_from(&mut rng_state.prng);
            rng_state
                .keys_generated
                .checked_add(1)
                .expect("too many keys generated");
            let public = secret.public();
            let owner = AccountOwner::from(public);
            Arc::get_mut(&mut self.keys).unwrap().insert(owner, secret);
            public
        }

        /// Creates a signature for the given `value` using the provided `owner`.
        fn sign(&self, owner: &AccountOwner, value: &CryptoHash) -> Option<AccountSignature> {
            let secret = self.keys.get(owner)?;
            let signature = secret.sign_prehash(*value);
            Some(signature)
        }

        /// Returns the public key corresponding to the given `owner`.
        fn get_public(&self, owner: &AccountOwner) -> Option<AccountPublicKey> {
            let secret = self.keys.get(owner)?;
            Some(secret.public())
        }

        /// Returnes whether the given `owner` is a known signer.
        fn contains_key(&self, owner: &AccountOwner) -> bool {
            self.keys.contains_key(owner)
        }

        fn clone_box(&self) -> Box<dyn Signer> {
            Box::new(self.clone())
        }
    }

    impl FromIterator<(AccountOwner, AccountSecretKey)> for InMemSigner {
        fn from_iter<T>(input: T) -> Self
        where
            T: IntoIterator<Item = (AccountOwner, AccountSecretKey)>,
        {
            InMemSigner {
                keys: Arc::new(BTreeMap::from_iter(input)),
                #[cfg(with_getrandom)]
                rng_state: Arc::new(Mutex::new(RngState::new(None, 0))),
            }
        }
    }

    impl Default for InMemSigner {
        fn default() -> Self {
            #[cfg(with_getrandom)]
            let signer = InMemSigner::new(None);
            #[cfg(not(with_getrandom))]
            let signer = InMemSigner::new();
            signer
        }
    }

    impl Clone for InMemSigner {
        fn clone(&self) -> Self {
            InMemSigner {
                keys: self.keys.clone(),
                #[cfg(with_getrandom)]
                rng_state: self.rng_state.clone(),
            }
        }
    }

    impl Serialize for InMemSigner {
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

            #[cfg(with_getrandom)]
            let rng_state = self.rng_state.lock().unwrap();

            let inner = Inner {
                keys: &self.keys(),
                #[cfg(with_getrandom)]
                prng_seed: rng_state.initial_prng_seed,
                #[cfg(with_getrandom)]
                keys_generated: rng_state.keys_generated,
            };

            Inner::serialize(&inner, serializer)
        }
    }

    impl<'de> Deserialize<'de> for InMemSigner {
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

            let signer = InMemSigner {
                keys: Arc::new(keys),
                #[cfg(with_getrandom)]
                rng_state: Arc::new(Mutex::new(RngState::new(
                    inner.prng_seed,
                    inner.keys_generated,
                ))),
            };
            Ok(signer)
        }
    }
}
