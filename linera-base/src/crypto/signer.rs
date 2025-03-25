// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

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
