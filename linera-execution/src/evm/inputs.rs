// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the input of functions, that is selectors,
//! constructor argument and instantiation argument.

use alloy_primitives::Bytes;
use linera_base::{
    crypto::CryptoHash,
    data_types::StreamUpdate,
    ensure,
    identifiers::{ApplicationId, ChainId, GenericApplicationId, StreamId, StreamName},
};
use revm_primitives::{address, Address, B256, U256};

use crate::EvmExecutionError;

alloy_sol_types::sol! {
    struct InternalApplicationId {
        bytes32 application_description_hash;
    }

    struct InternalGenericApplicationId {
        uint8 choice;
        InternalApplicationId user;
    }

    struct InternalStreamName {
        bytes stream_name;
    }

    struct InternalStreamId {
        InternalGenericApplicationId application_id;
        InternalStreamName stream_name;
    }

    struct InternalChainId {
        bytes32 value;
    }

    struct InternalStreamUpdate {
        InternalChainId chain_id;
        InternalStreamId stream_id;
        uint32 previous_index;
        uint32 next_index;
    }

    function process_streams(InternalStreamUpdate[] internal_streams);
}

fn crypto_hash_to_internal_crypto_hash(hash: CryptoHash) -> B256 {
    let hash = <[u64; 4]>::from(hash);
    let hash = linera_base::crypto::u64_array_to_be_bytes(hash);
    hash.into()
}

impl From<ApplicationId> for InternalApplicationId {
    fn from(application_id: ApplicationId) -> InternalApplicationId {
        let application_description_hash =
            crypto_hash_to_internal_crypto_hash(application_id.application_description_hash);
        InternalApplicationId {
            application_description_hash,
        }
    }
}

impl From<GenericApplicationId> for InternalGenericApplicationId {
    fn from(generic_application_id: GenericApplicationId) -> InternalGenericApplicationId {
        match generic_application_id {
            GenericApplicationId::System => {
                let application_description_hash = B256::ZERO;
                InternalGenericApplicationId {
                    choice: 0,
                    user: InternalApplicationId {
                        application_description_hash,
                    },
                }
            }
            GenericApplicationId::User(application_id) => InternalGenericApplicationId {
                choice: 1,
                user: application_id.into(),
            },
        }
    }
}

impl From<ChainId> for InternalChainId {
    fn from(chain_id: ChainId) -> InternalChainId {
        let value = crypto_hash_to_internal_crypto_hash(chain_id.0);
        InternalChainId { value }
    }
}

impl From<StreamName> for InternalStreamName {
    fn from(stream_name: StreamName) -> InternalStreamName {
        let stream_name = Bytes::from(stream_name.0);
        InternalStreamName { stream_name }
    }
}

impl From<StreamId> for InternalStreamId {
    fn from(stream_id: StreamId) -> InternalStreamId {
        let application_id = stream_id.application_id.into();
        let stream_name = stream_id.stream_name.into();
        InternalStreamId {
            application_id,
            stream_name,
        }
    }
}

impl From<StreamUpdate> for InternalStreamUpdate {
    fn from(stream_update: StreamUpdate) -> InternalStreamUpdate {
        let chain_id = stream_update.chain_id.into();
        let stream_id = stream_update.stream_id.into();
        InternalStreamUpdate {
            chain_id,
            stream_id,
            previous_index: stream_update.previous_index,
            next_index: stream_update.next_index,
        }
    }
}

// This is the precompile address that contains the Linera specific
// functionalities accessed from the EVM.
pub(crate) const PRECOMPILE_ADDRESS: Address = address!("000000000000000000000000000000000000000b");

// This is the zero address used when no address can be obtained from `authenticated_owner`
// and `authenticated_caller_id`. This scenario does not occur if an Address20 user calls or
// if an EVM contract calls another EVM contract.
pub(crate) const ZERO_ADDRESS: Address = address!("0000000000000000000000000000000000000000");

// This is the address being used for service calls.
pub(crate) const SERVICE_ADDRESS: Address = address!("0000000000000000000000000000000000002000");

/// This is the address used for getting ethers and transfering them to.
pub(crate) const FAUCET_ADDRESS: Address = address!("0000000000000000000000000000000000004000");
pub(crate) const FAUCET_BALANCE: U256 = U256::from_limbs([
    0xffffffffffffffff,
    0xffffffffffffffff,
    0xffffffffffffffff,
    0x7fffffffffffffff,
]);

/// This is the selector of `execute_message` that should be called
/// only from a submitted message
pub(crate) const EXECUTE_MESSAGE_SELECTOR: &[u8] = &[173, 125, 234, 205];

/// This is the selector of `process_streams` that should be called
/// only from a submitted message
pub(crate) const PROCESS_STREAMS_SELECTOR: &[u8] = &[254, 72, 102, 28];

/// This is the selector of `instantiate` that should be called
/// only when creating a new instance of a shared contract
pub(crate) const INSTANTIATE_SELECTOR: &[u8] = &[156, 163, 60, 158];

pub(crate) fn forbid_execute_operation_origin(vec: &[u8]) -> Result<(), EvmExecutionError> {
    ensure!(
        vec != EXECUTE_MESSAGE_SELECTOR,
        EvmExecutionError::IllegalOperationCall("function execute_message".to_string(),)
    );
    ensure!(
        vec != PROCESS_STREAMS_SELECTOR,
        EvmExecutionError::IllegalOperationCall("function process_streams".to_string(),)
    );
    ensure!(
        vec != INSTANTIATE_SELECTOR,
        EvmExecutionError::IllegalOperationCall("function instantiate".to_string(),)
    );
    Ok(())
}

pub(crate) fn ensure_message_length(
    actual_length: usize,
    min_length: usize,
) -> Result<(), EvmExecutionError> {
    ensure!(
        actual_length >= min_length,
        EvmExecutionError::OperationIsTooShort
    );
    Ok(())
}

pub(crate) fn ensure_selector_presence(
    module: &[u8],
    selector: &[u8],
    fct_name: &str,
) -> Result<(), EvmExecutionError> {
    ensure!(
        has_selector(module, selector),
        EvmExecutionError::MissingFunction(fct_name.to_string())
    );
    Ok(())
}

pub(crate) fn has_selector(module: &[u8], selector: &[u8]) -> bool {
    let push4 = 0x63; // An EVM instruction
    let mut vec = vec![push4];
    vec.extend(selector);
    module.windows(5).any(|window| window == vec)
}

pub(crate) fn get_revm_instantiation_bytes(value: Vec<u8>) -> Vec<u8> {
    use alloy_primitives::Bytes;
    use alloy_sol_types::{sol, SolCall};
    sol! {
        function instantiate(bytes value);
    }
    let bytes = Bytes::from(value);
    let argument = instantiateCall { value: bytes };
    argument.abi_encode()
}

pub(crate) fn get_revm_execute_message_bytes(value: Vec<u8>) -> Vec<u8> {
    use alloy_primitives::Bytes;
    use alloy_sol_types::{sol, SolCall};
    sol! {
        function execute_message(bytes value);
    }
    let value = Bytes::from(value);
    let argument = execute_messageCall { value };
    argument.abi_encode()
}

pub(crate) fn get_revm_process_streams_bytes(streams: Vec<StreamUpdate>) -> Vec<u8> {
    use alloy_sol_types::SolCall;

    let internal_streams = streams.into_iter().map(StreamUpdate::into).collect();

    let fct_call = process_streamsCall { internal_streams };
    fct_call.abi_encode()
}

#[cfg(test)]
mod tests {
    use revm_primitives::keccak256;

    use crate::evm::inputs::{
        process_streamsCall, EXECUTE_MESSAGE_SELECTOR, INSTANTIATE_SELECTOR,
        PROCESS_STREAMS_SELECTOR,
    };

    // The function keccak256 is not const so we cannot build the execute_message
    // selector directly.
    #[test]
    fn check_execute_message_selector() {
        let selector = &keccak256("execute_message(bytes)".as_bytes())[..4];
        assert_eq!(selector, EXECUTE_MESSAGE_SELECTOR);
    }

    #[test]
    fn check_process_streams_selector() {
        use alloy_sol_types::SolCall;
        assert_eq!(
            process_streamsCall::SIGNATURE,
            "process_streams(((bytes32),((uint8,(bytes32)),(bytes)),uint32,uint32)[])"
        );
        assert_eq!(process_streamsCall::SELECTOR, PROCESS_STREAMS_SELECTOR);
    }

    #[test]
    fn check_instantiate_selector() {
        let selector = &keccak256("instantiate(bytes)".as_bytes())[..4];
        assert_eq!(selector, INSTANTIATE_SELECTOR);
    }
}
