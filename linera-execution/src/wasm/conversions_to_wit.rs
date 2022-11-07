use super::runtime::application;
use crate::{CalleeContext, EffectContext, EffectId, OperationContext, QueryContext, SessionId};
use linera_base::{crypto::HashValue, messages::ChainId};

impl From<OperationContext> for application::OperationContext {
    fn from(host: OperationContext) -> Self {
        application::OperationContext {
            chain_id: host.chain_id.into(),
            height: host.height.0,
            index: host
                .index
                .try_into()
                .expect("Operation index should fit in an `u64`"),
        }
    }
}

impl From<EffectContext> for application::EffectContext {
    fn from(host: EffectContext) -> Self {
        application::EffectContext {
            chain_id: host.chain_id.into(),
            height: host.height.0,
            effect_id: host.effect_id.into(),
        }
    }
}

impl From<EffectId> for application::EffectId {
    fn from(host: EffectId) -> Self {
        application::EffectId {
            chain_id: host.chain_id.into(),
            height: host.height.0,
            index: host
                .index
                .try_into()
                .expect("Effect index should fit in an `u64`"),
        }
    }
}

impl From<CalleeContext> for application::CalleeContext {
    fn from(host: CalleeContext) -> Self {
        application::CalleeContext {
            chain_id: host.chain_id.into(),
            authenticated_caller_id: host.authenticated_caller_id.map(|app_id| app_id.0),
        }
    }
}

impl From<QueryContext> for application::QueryContext {
    fn from(host: QueryContext) -> Self {
        application::QueryContext {
            chain_id: host.chain_id.into(),
        }
    }
}

impl From<SessionId> for application::SessionId {
    fn from(host: SessionId) -> Self {
        application::SessionId {
            application_id: host.application_id.0,
            kind: host.kind,
            index: host.index,
        }
    }
}

impl From<ChainId> for application::ChainId {
    fn from(chain_id: ChainId) -> Self {
        chain_id.0.into()
    }
}

impl From<HashValue> for application::HashValue {
    fn from(hash_value: HashValue) -> Self {
        let bytes = hash_value.as_bytes();

        application::HashValue {
            part1: u64::from_le_bytes(bytes[0..8].try_into().expect("incorrect indices")),
            part2: u64::from_le_bytes(bytes[8..16].try_into().expect("incorrect indices")),
            part3: u64::from_le_bytes(bytes[16..24].try_into().expect("incorrect indices")),
            part4: u64::from_le_bytes(bytes[24..32].try_into().expect("incorrect indices")),
            part5: u64::from_le_bytes(bytes[32..40].try_into().expect("incorrect indices")),
            part6: u64::from_le_bytes(bytes[40..48].try_into().expect("incorrect indices")),
            part7: u64::from_le_bytes(bytes[48..56].try_into().expect("incorrect indices")),
            part8: u64::from_le_bytes(bytes[56..64].try_into().expect("incorrect indices")),
        }
    }
}
