// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::{BcsSignable, CryptoHash},
    data_types::{BlockHeight, ChainId, EffectId},
};
use linera_execution::{BytecodeLocation, UserApplicationDescription};
use serde::{Deserialize, Serialize};

pub fn create_dummy_user_application_description() -> UserApplicationDescription {
    let chain_id = ChainId::root(1);
    let certificate_hash = CryptoHash::new(&FakeCertificate);
    UserApplicationDescription {
        bytecode_id: EffectId {
            chain_id,
            height: BlockHeight(1),
            index: 0,
        }
        .into(),
        bytecode_location: BytecodeLocation {
            certificate_hash,
            operation_index: 0,
        },
        creation: EffectId {
            chain_id,
            height: BlockHeight(1),
            index: 1,
        },
        required_application_ids: vec![],
        parameters: vec![],
    }
}

#[derive(Deserialize, Serialize)]
pub struct FakeCertificate;

impl BcsSignable for FakeCertificate {}
