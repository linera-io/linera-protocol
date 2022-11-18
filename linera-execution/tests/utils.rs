use linera_base::messages::{ApplicationId, BlockHeight, BytecodeId, ChainId, EffectId};

pub fn create_dummy_user_application_id() -> ApplicationId {
    let chain_id = ChainId::root(1);
    ApplicationId::User {
        bytecode: BytecodeId(EffectId {
            chain_id,
            height: BlockHeight(0),
            index: 0,
        }),
        creation: EffectId {
            chain_id,
            height: BlockHeight(1),
            index: 0,
        },
    }
}
