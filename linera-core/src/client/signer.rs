// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use linera_base::{
    crypto::{AccountPublicKey, AccountSecretKey, AccountSignature, CryptoHash, CryptoRng, Signer},
    identifiers::AccountOwner,
};
use serde::{Deserialize, Serialize};

/// In-memory signer.
pub struct InMemSigner {
    prng: Box<dyn CryptoRng>,
    keys: BTreeMap<AccountOwner, AccountSecretKey>,
    // Kept around for deterministic reconstruction of the RNG
    // across the persistence boundary.
    initial_prng_seed: Option<u64>,
    keys_generated: u64,
}

impl InMemSigner {
    /// Creates a new `InMemSigner`.
    pub fn new(prng_seed: Option<u64>) -> Self {
        let prng: Box<dyn CryptoRng> = prng_seed.into();
        InMemSigner {
            prng,
            keys: BTreeMap::new(),
            initial_prng_seed: prng_seed,
            keys_generated: 0,
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

impl<T: IntoIterator<Item = (AccountOwner, AccountSecretKey)>> From<T> for InMemSigner {
    fn from(input: T) -> Self {
        InMemSigner {
            initial_prng_seed: None,
            keys_generated: 0,
            prng: None.into(),
            keys: BTreeMap::from_iter(input),
        }
    }
}

impl Default for InMemSigner {
    fn default() -> Self {
        InMemSigner::new(None)
    }
}

impl Clone for InMemSigner {
    fn clone(&self) -> Self {
        let mut keys = BTreeMap::new();
        for (owner, secret) in self.keys.iter() {
            keys.insert(*owner, secret.copy());
        }
        let prng = self.initial_prng_seed.into();
        InMemSigner {
            initial_prng_seed: self.initial_prng_seed,
            keys_generated: self.keys_generated,
            prng,
            keys,
        }
    }
}

impl Signer for InMemSigner {
    /// Generates a new key pair from Signer's RNG. Use with care.
    #[cfg(with_getrandom)]
    fn generate_new(&mut self) -> AccountPublicKey {
        let secret = AccountSecretKey::generate_from(&mut self.prng);
        self.keys_generated
            .checked_add(1)
            .expect("too many keys generated");
        let public = secret.public();
        let owner = AccountOwner::from(public);
        self.keys.insert(owner, secret);
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

impl Serialize for InMemSigner {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Inner<'a> {
            prng_seed: Option<u64>,
            keys_generated: u64,
            keys: &'a Vec<(AccountOwner, Vec<u8>)>,
        }
        let inner = Inner {
            prng_seed: self.initial_prng_seed,
            keys_generated: self.keys_generated,
            keys: &self.keys(),
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
            prng_seed: Option<u64>,
            keys_generated: u64,
            keys: Vec<(AccountOwner, Vec<u8>)>,
        }
        let inner = Inner::deserialize(deserializer)?;

        let mut prng: Box<dyn CryptoRng> = inner.prng_seed.into();
        for _ in 0..inner.keys_generated {
            // Rebuild the PRNG state by generating dummy values.
            let _ = prng.next_u64();
        }

        Ok(InMemSigner {
            initial_prng_seed: inner.prng_seed,
            keys_generated: inner.keys_generated,
            prng,
            keys: inner
                .keys
                .into_iter()
                .map(|(owner, secret)| {
                    let secret =
                        serde_json::from_slice(&secret).map_err(serde::de::Error::custom)?;
                    Ok((owner, secret))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?,
        })
    }
}
