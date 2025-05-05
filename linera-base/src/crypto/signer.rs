// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
pub use in_mem::InMemorySigner;

use super::CryptoHash;
use crate::{
    crypto::{AccountPublicKey, AccountSignature},
    identifiers::AccountOwner,
};

/// A trait for signing keys.
#[async_trait]
pub trait Signer: Send + Sync {
    /// Creates a signature for the given `value` using the provided `owner`.
    // DEV: We sign `CryptoHash` type, rather than `&[u8]` to make sure we don't sign
    // things accidentally. See [`CryptoHash::new`] for how the type's name is included
    // in the resulting hash, providing the canonicity of the hashing process.
    async fn sign(
        &self,
        owner: &AccountOwner,
        value: &CryptoHash,
    ) -> Result<AccountSignature, Box<dyn std::error::Error>>;

    /// Returns the public key corresponding to the given `owner`.
    async fn get_public_key(
        &self,
        owner: &AccountOwner,
    ) -> Result<AccountPublicKey, Box<dyn std::error::Error>>;

    /// Returns whether the given `owner` is a known signer.
    async fn contains_key(&self, owner: &AccountOwner) -> Result<bool, Box<dyn std::error::Error>>;
}

#[async_trait]
impl Signer for Box<dyn Signer> {
    async fn sign(
        &self,
        owner: &AccountOwner,
        value: &CryptoHash,
    ) -> Result<AccountSignature, Box<dyn std::error::Error>> {
        (**self).sign(owner, value).await
    }

    async fn get_public_key(
        &self,
        owner: &AccountOwner,
    ) -> Result<AccountPublicKey, Box<dyn std::error::Error>> {
        (**self).get_public_key(owner).await
    }

    async fn contains_key(&self, owner: &AccountOwner) -> Result<bool, Box<dyn std::error::Error>> {
        (**self).contains_key(owner).await
    }
}

/// In-memory implementation of the [`Signer`] trait.
mod in_mem {
    use std::{
        collections::BTreeMap,
        sync::{Arc, RwLock},
    };

    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    #[cfg(with_getrandom)]
    use crate::crypto::CryptoRng;
    use crate::{
        crypto::{AccountPublicKey, AccountSecretKey, AccountSignature, CryptoHash, Signer},
        identifiers::AccountOwner,
    };

    /// In-memory signer.
    #[derive(Clone)]
    pub struct InMemorySigner(Arc<RwLock<InMemSignerInner>>);

    impl InMemorySigner {
        /// Creates a new [`InMemorySigner`] seeded with `prng_seed`.
        /// If `prng_seed` is `None`, an `OsRng` will be used.
        #[cfg(with_getrandom)]
        pub fn new(prng_seed: Option<u64>) -> Self {
            InMemorySigner(Arc::new(RwLock::new(InMemSignerInner::new(prng_seed))))
        }

        /// Creates a new [`InMemorySigner`].
        #[cfg(not(with_getrandom))]
        pub fn new() -> Self {
            InMemorySigner(Arc::new(RwLock::new(InMemSignerInner::new())))
        }

        /// Generates a new key pair from Signer's RNG. Use with care.
        #[cfg(with_getrandom)]
        pub fn generate_new(&mut self) -> AccountPublicKey {
            let mut inner = self.0.write().unwrap();
            let secret = AccountSecretKey::generate_from(&mut inner.rng_state.prng);
            if inner.rng_state.testing_seed.is_some() {
                // Generate a new testing seed for the case when we need to store the PRNG state.
                // It provides a "forward-secrecy" property for the testing seed.
                // We do not do that for the case when `testing_seed` is `None`, because
                // we default to the usage of OsRng in that case.
                inner.rng_state.testing_seed = Some(inner.rng_state.prng.next_u64());
            }
            let public = secret.public();
            let owner = AccountOwner::from(public);
            inner.keys.insert(owner, secret);
            public
        }

        /// Returns the public key corresponding to the given `owner`.
        pub fn keys(&self) -> Vec<(AccountOwner, Vec<u8>)> {
            let inner = self.0.read().unwrap();
            inner.keys()
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
        #[cfg(with_getrandom)]
        testing_seed: Option<u64>,
    }

    #[cfg(with_getrandom)]
    impl RngState {
        fn new(prng_seed: Option<u64>) -> Self {
            let prng: Box<dyn CryptoRng> = prng_seed.into();
            RngState {
                prng,
                #[cfg(with_getrandom)]
                testing_seed: prng_seed,
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
                rng_state: RngState::new(prng_seed),
            }
        }

        /// Creates a new `InMemSignerInner`.
        #[cfg(not(with_getrandom))]
        pub fn new() -> Self {
            InMemSignerInner {
                keys: BTreeMap::new(),
            }
        }

        pub fn keys(&self) -> Vec<(AccountOwner, Vec<u8>)> {
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

    #[async_trait]
    impl Signer for InMemorySigner {
        /// Creates a signature for the given `value` using the provided `owner`.
        async fn sign(
            &self,
            owner: &AccountOwner,
            value: &CryptoHash,
        ) -> Result<AccountSignature, Box<dyn std::error::Error>> {
            let inner = self.0.read().unwrap();
            if let Some(secret) = inner.keys.get(owner) {
                let signature = secret.sign_prehash(*value);
                Ok(signature)
            } else {
                Err("No key found for the given owner".into())
            }
        }

        /// Returns the public key corresponding to the given `owner`.
        async fn get_public_key(
            &self,
            owner: &AccountOwner,
        ) -> Result<AccountPublicKey, Box<dyn std::error::Error>> {
            let inner = self.0.read().unwrap();
            match inner.keys.get(owner).map(|s| s.public()) {
                Some(public) => Ok(public),
                None => Err("No key found for the given owner".into()),
            }
        }

        /// Returns whether the given `owner` is a known signer.
        async fn contains_key(
            &self,
            owner: &AccountOwner,
        ) -> Result<bool, Box<dyn std::error::Error>> {
            let inner = self.0.read().unwrap();
            Ok(inner.keys.contains_key(owner))
        }
    }

    impl FromIterator<(AccountOwner, AccountSecretKey)> for InMemorySigner {
        fn from_iter<T>(input: T) -> Self
        where
            T: IntoIterator<Item = (AccountOwner, AccountSecretKey)>,
        {
            InMemorySigner(Arc::new(RwLock::new(InMemSignerInner {
                keys: BTreeMap::from_iter(input),
                #[cfg(with_getrandom)]
                rng_state: RngState::new(None),
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

    impl Serialize for InMemorySigner {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let inner = self.0.read().unwrap();
            InMemSignerInner::serialize(&*inner, serializer)
        }
    }

    impl<'de> Deserialize<'de> for InMemorySigner {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let inner = InMemSignerInner::deserialize(deserializer)?;
            Ok(InMemorySigner(Arc::new(RwLock::new(inner))))
        }
    }

    impl Serialize for InMemSignerInner {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            #[derive(Serialize, Debug)]
            struct Inner<'a> {
                keys: &'a Vec<(AccountOwner, Vec<u8>)>,
                #[cfg(with_getrandom)]
                prng_seed: Option<u64>,
            }

            #[cfg(with_getrandom)]
            let prng_seed = self.rng_state.testing_seed;

            let inner = Inner {
                keys: &self.keys(),
                #[cfg(with_getrandom)]
                prng_seed,
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
                rng_state: RngState::new(inner.prng_seed),
            };
            Ok(signer)
        }
    }
}
