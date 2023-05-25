// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::{BcsSignable, CryptoHash},
    data_types::BlockHeight,
    identifiers::{BytecodeId, ChainId, EffectId},
};
use linera_execution::{ApplicationDescription, BytecodeLocation};
use serde::{Deserialize, Serialize};

pub fn create_dummy_user_application_description() -> ApplicationDescription {
    let chain_id = ChainId::root(1);
    let certificate_hash = CryptoHash::new(&FakeCertificate);
    ApplicationDescription {
        bytecode_id: BytecodeId::new(EffectId {
            chain_id,
            height: BlockHeight(1),
            index: 0,
        }),
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
