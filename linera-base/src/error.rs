// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::crypto::CryptoError;
use thiserror::Error;

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            return Err($e);
        }
    };
}

#[derive(Debug, Error)]
/// Custom error type.
pub enum Error {
    #[error(transparent)]
    CryptoError(#[from] CryptoError),

    // Algorithmic operations
    #[error("Sequence number overflow")]
    SequenceOverflow,
    #[error("Sequence number underflow")]
    SequenceUnderflow,
}
