// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[path = "./types.rs"]
pub mod types;

use self::types::{AccountOwner, Nonce};
use linera_sdk::{
    crypto::{BcsSignable, CryptoError, PublicKey, Signature},
    ApplicationId, ChainId,
};
use serde::{Deserialize, Serialize};

/// A cross-application call.
#[derive(Deserialize, Serialize)]
pub enum ApplicationCall {
    /// A request for the application's account balance.
    Balance,
    /// A transfer from the application's account.
    Transfer(ApplicationTransfer),
    /// A signed transfer operation delegated to the application.
    Delegated(SignedTransfer),
}

/// A cross-application transfer request.
#[derive(Deserialize, Serialize)]
pub enum ApplicationTransfer {
    /// A static transfer to a specific destination.
    Static(Transfer),
    /// A dynamic transfer into a session, that can then be credited to destinations later.
    Dynamic(u128),
}

impl ApplicationTransfer {
    /// The amount of tokens to be transfered.
    pub fn amount(&self) -> u128 {
        match self {
            ApplicationTransfer::Static(transfer) => transfer.amount,
            ApplicationTransfer::Dynamic(amount) => *amount,
        }
    }
}

/// The transfer operation.
#[derive(Deserialize, Serialize)]
pub struct SignedTransfer {
    pub source: PublicKey,
    pub signature: Signature,
    pub payload: SignedTransferPayload,
}

/// The payload to be signed for a signed transfer.
///
/// Contains extra meta-data in to be included in signature to ensure that the transfer can't be
/// replayed:
///
/// - on the same chain
/// - on different chains
/// - on different tokens
#[derive(Debug, Deserialize, Serialize)]
pub struct SignedTransferPayload {
    pub token_id: ApplicationId,
    pub source_chain: ChainId,
    pub nonce: Nonce,
    pub transfer: Transfer,
}

/// A transfer payload.
#[derive(Debug, Deserialize, Serialize)]
pub struct Transfer {
    pub destination_account: AccountOwner,
    pub destination_chain: ChainId,
    pub amount: u128,
}

impl SignedTransfer {
    /// Checks that the [`SignedTransfer`] is correctly signed.
    ///
    /// If correctly signed, returns the source of the transfer and the [`SignedTransferPayload`].
    pub fn check_signature(self) -> Result<(AccountOwner, SignedTransferPayload), CryptoError> {
        self.signature.check(&self.payload, self.source)?;

        Ok((AccountOwner::Key(self.source), self.payload))
    }
}

impl BcsSignable for SignedTransferPayload {}
