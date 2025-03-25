// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use serde::{ser::SerializeMap, Deserialize, Serialize};

use crate::{
    crypto::{AccountPublicKey, AccountSecretKey, AccountSignature, BcsSignable},
    identifiers::AccountOwner,
};

/// Wrapper around bytes that can be signed.
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct SignableBytes(Vec<u8>);
impl SignableBytes {
    /// Creates a new `SignableBytes` from the given bytes.
    pub fn new(bytes: Vec<u8>) -> Self {
        SignableBytes(bytes)
    }

    /// Returns the inner bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
impl BcsSignable<'_> for SignableBytes {}

/// A trait for signing keys.
#[cfg_attr(not(web), trait_variant::make(Send + Sync))]
pub trait Signer {
    /// Generates a new signing key for the Self type.
    /// New secret key is inserted into Signer's memory and the `AccountPublicKey` is returned.
    #[cfg(all(with_testing, with_getrandom))]
    fn generate_new(&mut self) -> AccountPublicKey;

    /// Creates a signature for the given `value` using the provided `owner`.
    fn sign(&self, owner: &AccountOwner, value: &SignableBytes) -> Option<AccountSignature>;

    /// Returns the public key corresponding to the given `owner`.
    fn get_public(&self, owner: &AccountOwner) -> Option<AccountPublicKey>;

    /// Returnes whether the given `owner` is a known signer.
    fn contains_key(&self, owner: &AccountOwner) -> bool;

    /// Removes the key for the given `owner`.
    fn remove(&mut self, owner: &AccountOwner) -> bool;

    /// Returns a clone of the `Signer` as a boxed trait object.
    fn clone_box(&self) -> Box<dyn Signer>;

    /// Returns an iterator over the keys in the signer.
    fn keys(&self) -> Vec<(AccountOwner, Vec<u8>)>;
}

impl Serialize for Box<dyn Signer> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let keys = self.keys();
        let mut map = serializer.serialize_map(Some(keys.len()))?;
        for (owner, secret) in keys {
            map.serialize_entry(&owner, &secret)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Box<dyn Signer> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let keys: Vec<(AccountOwner, Vec<u8>)> = Deserialize::deserialize(deserializer)?;
        let mut signer = InMemSigner::new();
        for (owner, secret) in keys {
            let secret = bcs::from_bytes(&secret).map_err(serde::de::Error::custom)?;
            signer.0.insert(owner, secret);
        }
        Ok(Box::new(signer))
    }
}

impl Clone for Box<dyn Signer> {
    fn clone(&self) -> Box<dyn Signer> {
        self.clone_box()
    }
}

/// In-memory signer.
pub struct InMemSigner(BTreeMap<AccountOwner, AccountSecretKey>);
impl InMemSigner {
    /// Creates a new `InMemSigner`.
    pub fn new() -> Self {
        InMemSigner(BTreeMap::new())
    }
}

impl<T: IntoIterator<Item = (AccountOwner, AccountSecretKey)>> From<T> for InMemSigner {
    fn from(input: T) -> Self {
        InMemSigner(BTreeMap::from_iter(input))
    }
}

impl Default for InMemSigner {
    fn default() -> Self {
        InMemSigner::new()
    }
}

impl Clone for InMemSigner {
    fn clone(&self) -> Self {
        let mut map = BTreeMap::new();
        for (owner, secret) in self.0.iter() {
            map.insert(*owner, secret.copy());
        }
        InMemSigner(map)
    }
}

impl Signer for InMemSigner {
    /// Generates a new signing key for the Self type.
    #[cfg(all(with_testing, with_getrandom))]
    fn generate_new(&mut self) -> AccountPublicKey {
        let secret = AccountSecretKey::generate();
        let public = secret.public();
        let owner = AccountOwner::from(public);
        self.0.insert(owner, secret);
        public
    }

    /// Creates a signature for the given `value` using the provided `owner`.
    fn sign(&self, owner: &AccountOwner, value: &SignableBytes) -> Option<AccountSignature> {
        let secret = self.0.get(owner)?;
        let signature = secret.sign(value);
        Some(signature)
    }

    /// Returns the public key corresponding to the given `owner`.
    fn get_public(&self, owner: &AccountOwner) -> Option<AccountPublicKey> {
        let secret = self.0.get(owner)?;
        Some(secret.public())
    }

    /// Returnes whether the given `owner` is a known signer.
    fn contains_key(&self, owner: &AccountOwner) -> bool {
        self.0.contains_key(owner)
    }

    /// Removes the key for the given `owner`.
    fn remove(&mut self, owner: &AccountOwner) -> bool {
        self.0.remove(owner).is_some()
    }

    fn clone_box(&self) -> Box<dyn Signer> {
        Box::new(self.clone())
    }

    fn keys(&self) -> Vec<(AccountOwner, Vec<u8>)> {
        self.0
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

impl Default for Box<dyn Signer> {
    fn default() -> Self {
        Box::new(InMemSigner::new())
    }
}
