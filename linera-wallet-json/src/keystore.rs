// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use linera_base::{
    crypto::{AccountPublicKey, AccountSignature, CryptoHash, InMemorySigner, Signer},
    identifiers::AccountOwner,
};
use linera_persistent::{self as persistent, Persist as _};

/// A persistent keystore backed by a JSON file with exclusive locking.
pub struct Keystore(persistent::File<InMemorySigner>);

impl Signer for Keystore {
    type Error = <InMemorySigner as Signer>::Error;

    async fn sign(
        &self,
        owner: &AccountOwner,
        value: &CryptoHash,
    ) -> Result<AccountSignature, Self::Error> {
        (*self.0).sign(owner, value).await
    }

    async fn contains_key(&self, owner: &AccountOwner) -> Result<bool, Self::Error> {
        (*self.0).contains_key(owner).await
    }
}

impl Keystore {
    /// Reads an existing keystore from disk.
    pub fn read(path: &Path) -> Result<Self, persistent::file::Error> {
        Ok(Self(persistent::File::read(path)?))
    }

    /// Creates a new keystore at `path`. If `testing_prng_seed` is provided,
    /// uses deterministic key generation.
    pub fn create(
        path: &Path,
        testing_prng_seed: Option<u64>,
    ) -> Result<Self, persistent::file::Error> {
        Ok(Self(persistent::File::read_or_create(path, || {
            Ok(InMemorySigner::new(testing_prng_seed))
        })?))
    }

    /// Generates a new key pair, persists the keystore, and returns the public key.
    pub async fn generate_key(&mut self) -> Result<AccountPublicKey, persistent::file::Error> {
        let key = self.0.generate_new();
        self.0.persist().await?;
        Ok(key)
    }

    /// Generates `count` new key pairs, persists the keystore, and returns the public keys.
    pub async fn generate_keys(
        &mut self,
        count: usize,
    ) -> Result<Vec<AccountPublicKey>, persistent::file::Error> {
        let keys: Vec<_> = std::iter::repeat_with(|| self.0.generate_new())
            .take(count)
            .collect();
        self.0.persist().await?;
        Ok(keys)
    }

    /// Saves the keystore to disk.
    pub async fn save(&mut self) -> Result<(), persistent::file::Error> {
        self.0.persist().await
    }

    /// Consumes the keystore and returns the inner signer.
    pub fn into_signer(self) -> InMemorySigner {
        self.0.into_value()
    }
}
