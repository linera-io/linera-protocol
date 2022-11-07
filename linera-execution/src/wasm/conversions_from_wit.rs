use super::runtime::application;
use crate::{ApplicationCallResult, NewSession, RawExecutionResult, SessionCallResult};
use linera_base::{
    crypto::HashValue,
    messages::{ChainId, Destination},
};

impl From<application::SessionCallResult> for SessionCallResult {
    fn from(result: application::SessionCallResult) -> Self {
        SessionCallResult {
            inner: result.inner.into(),
            close_session: result.data.is_some(),
        }
    }
}

impl From<application::ApplicationCallResult> for ApplicationCallResult {
    fn from(result: application::ApplicationCallResult) -> Self {
        let create_sessions = result
            .create_sessions
            .into_iter()
            .map(NewSession::from)
            .collect();

        ApplicationCallResult {
            create_sessions,
            execution_result: result.execution_result.into(),
            value: result.value,
        }
    }
}

impl From<application::ExecutionResult> for RawExecutionResult<Vec<u8>> {
    fn from(result: application::ExecutionResult) -> Self {
        let effects = result
            .effects
            .into_iter()
            .map(|(destination, effect)| (destination.into(), effect))
            .collect();

        let subscribe = result
            .subscribe
            .into_iter()
            .map(|(channel_id, chain_id)| (channel_id, chain_id.into()))
            .collect();

        let unsubscribe = result
            .unsubscribe
            .into_iter()
            .map(|(channel_id, chain_id)| (channel_id, chain_id.into()))
            .collect();

        RawExecutionResult {
            effects,
            subscribe,
            unsubscribe,
        }
    }
}

impl From<application::Destination> for Destination {
    fn from(guest: application::Destination) -> Self {
        match guest {
            application::Destination::Recipient(chain_id) => {
                Destination::Recipient(chain_id.into())
            }
            application::Destination::Subscribers(channel_id) => {
                Destination::Subscribers(channel_id)
            }
        }
    }
}

impl From<application::SessionResult> for NewSession {
    fn from(guest: application::SessionResult) -> Self {
        NewSession {
            kind: guest.kind,
            data: guest.data,
        }
    }
}

impl From<application::HashValue> for HashValue {
    fn from(guest: application::HashValue) -> Self {
        let mut bytes = [0u8; 64];

        bytes[0..8].copy_from_slice(&guest.part1.to_le_bytes());
        bytes[8..16].copy_from_slice(&guest.part2.to_le_bytes());
        bytes[16..24].copy_from_slice(&guest.part3.to_le_bytes());
        bytes[24..32].copy_from_slice(&guest.part4.to_le_bytes());
        bytes[32..40].copy_from_slice(&guest.part5.to_le_bytes());
        bytes[40..48].copy_from_slice(&guest.part6.to_le_bytes());
        bytes[48..56].copy_from_slice(&guest.part7.to_le_bytes());
        bytes[56..64].copy_from_slice(&guest.part8.to_le_bytes());

        HashValue::try_from(&bytes[..]).expect("Incorrect byte count for `HashValue`")
    }
}

impl From<application::ChainId> for ChainId {
    fn from(guest: application::ChainId) -> Self {
        ChainId(guest.into())
    }
}
