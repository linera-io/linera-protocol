use linera_base::{
    crypto::{BcsSignable, HashValue},
    messages::{BlockHeight, ChainId, EffectId},
};
use linera_execution::{BytecodeLocation, UserApplicationDescription};
use serde::{Deserialize, Serialize};

pub fn create_dummy_user_application_description() -> UserApplicationDescription {
    let chain_id = ChainId::root(1);
    let certificate_hash = HashValue::new(&FakeCertificate);
    UserApplicationDescription {
        bytecode_id: EffectId {
            chain_id,
            height: BlockHeight(1),
            index: 0,
        }
        .into(),
        bytecode: BytecodeLocation {
            certificate_hash,
            operation_index: 0,
        },
        creation: EffectId {
            chain_id,
            height: BlockHeight(1),
            index: 1,
        },
        initialization_argument: vec![],
    }
}

#[derive(Deserialize, Serialize)]
pub struct FakeCertificate;

impl BcsSignable for FakeCertificate {}
