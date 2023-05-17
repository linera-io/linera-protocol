// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Conversions to WIT types from the original types.

use super::wit;
use linera_base::{
    crypto::CryptoHash,
    data_types::Amount,
    identifiers::{ApplicationId, ChainId, EffectId},
};

impl From<ChainId> for wit::CryptoHash {
    fn from(chain_id: ChainId) -> Self {
        chain_id.0.into()
    }
}

impl From<CryptoHash> for wit::CryptoHash {
    fn from(crypto_hash: CryptoHash) -> Self {
        let parts = <[u64; 4]>::from(crypto_hash);

        wit::CryptoHash {
            part1: parts[0],
            part2: parts[1],
            part3: parts[2],
            part4: parts[3],
        }
    }
}

impl From<ApplicationId> for wit::ApplicationId {
    fn from(application_id: ApplicationId) -> Self {
        wit::ApplicationId {
            bytecode_id: application_id.bytecode_id.0.into(),
            creation: application_id.creation.into(),
        }
    }
}

impl From<EffectId> for wit::EffectId {
    fn from(effect_id: EffectId) -> Self {
        wit::EffectId {
            chain_id: effect_id.chain_id.0.into(),
            height: effect_id.height.0,
            index: effect_id.index,
        }
    }
}

impl From<Amount> for wit::Amount {
    fn from(balance: Amount) -> Self {
        wit::Amount {
            lower_half: balance.lower_half(),
            upper_half: balance.upper_half(),
        }
    }
}

impl From<wit::LogLevel> for log::Level {
    fn from(level: wit::LogLevel) -> Self {
        match level {
            wit::LogLevel::Trace => log::Level::Trace,
            wit::LogLevel::Debug => log::Level::Debug,
            wit::LogLevel::Info => log::Level::Info,
            wit::LogLevel::Warn => log::Level::Warn,
            wit::LogLevel::Error => log::Level::Error,
        }
    }
}
