// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
An interface for cryptographic signers that can be used by the Linera client to sign blocks.
*/

use std::error::Error as StdError;

pub use in_mem::InMemorySigner;

use super::CryptoHash;
use crate::{crypto::AccountSignature, identifiers::AccountOwner};

cfg_if::cfg_if! {
    if #[cfg(web)] {
        #[doc(hidden)]
        pub trait TaskSendable {}
        impl<T> TaskSendable for T {}
    } else {
        #[doc(hidden)]
        pub trait TaskSendable: Send + Sync {}
        impl<T: Send + Sync> TaskSendable for T {}
    }
}

/// Errors that can be returned from signers.
pub trait Error: StdError + TaskSendable {}
impl<T: StdError + TaskSendable> Error for T {}

impl StdError for Box<dyn Error + '_> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        (**self).source()
    }
}

/// A trait for signing keys.
#[cfg_attr(not(web), trait_variant::make(Send))]
pub trait Signer {
    /// The type of errors arising from operations on this `Signer`.
    type Error: Error;

    /// Creates a signature for the given `value` using the provided `owner`.
    // DEV: We sign `CryptoHash` type, rather than `&[u8]` to make sure we don't sign
    // things accidentally. See [`CryptoHash::new`] for how the type's name is included
    // in the resulting hash, providing the canonicity of the hashing process.
    async fn sign(
        &self,
        owner: &AccountOwner,
        value: &CryptoHash,
    ) -> Result<AccountSignature, Self::Error>;

    /// Returns whether the given `owner` is a known signer.
    async fn contains_key(&self, owner: &AccountOwner) -> Result<bool, Self::Error>;
}

/// In-memory implementation of the [`Signer`] trait.
mod in_mem {
    use std::{
        collections::BTreeMap,
        sync::{Arc, RwLock},
    };

    use serde::{Deserialize, Serialize};

    #[cfg(with_getrandom)]
    use crate::crypto::{AccountPublicKey, CryptoRng};
    use crate::{
        crypto::{AccountSecretKey, AccountSignature, CryptoHash, Signer},
        identifiers::AccountOwner,
    };

    #[derive(Debug, thiserror::Error)]
    pub enum Error {
        #[error("no key found for the given owner")]
        NoSuchOwner,
    }

    /// In-memory signer.
    #[derive(Clone)]
    pub struct InMemorySigner(Arc<RwLock<InMemSignerInner>>);

    #[cfg(not(with_getrandom))]
    impl Default for InMemorySigner {
        fn default() -> Self {
            Self::new()
        }
    }

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

    impl Signer for InMemorySigner {
        type Error = Error;

        /// Creates a signature for the given `value` using the provided `owner`.
        async fn sign(
            &self,
            owner: &AccountOwner,
            value: &CryptoHash,
        ) -> Result<AccountSignature, Error> {
            let inner = self.0.read().unwrap();
            if let Some(secret) = inner.keys.get(owner) {
                let signature = secret.sign_prehash(*value);
                Ok(signature)
            } else {
                Err(Error::NoSuchOwner)
            }
        }

        /// Returns whether the given `owner` is a known signer.
        async fn contains_key(&self, owner: &AccountOwner) -> Result<bool, Error> {
            Ok(self.0.read().unwrap().keys.contains_key(owner))
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
