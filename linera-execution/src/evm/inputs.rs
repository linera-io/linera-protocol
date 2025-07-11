// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the input of functions, that is selectors,
//! constructor argument and instantiation argument.

use alloy_sol_types::SolCall;
use linera_base::{
    crypto::CryptoHash,
    data_types::{StreamUpdate, Amount},
    ensure,
    identifiers::{ApplicationId, ChainId, StreamName},
};
use revm_primitives::U256;
use serde::{Deserialize, Serialize};
use crate::{ExecutionError, EvmExecutionError};

/// This is the selector of the `execute_message` that should be called
/// only from a submitted message
pub(crate) const EXECUTE_MESSAGE_SELECTOR: &[u8] = &[173, 125, 234, 205];

/// This is the selector of the `process_streams` that should be called
/// only from a submitted message
pub(crate) const PROCESS_STREAMS_SELECTOR: &[u8] = &[227, 9, 189, 153];

/// This is the selector of the `instantiate` that should be called
/// only when creating a new instance of a shared contract
pub(crate) const INSTANTIATE_SELECTOR: &[u8] = &[156, 163, 60, 158];

pub(crate) fn forbid_execute_operation_origin(vec: &[u8]) -> Result<(), EvmExecutionError> {
    if vec == EXECUTE_MESSAGE_SELECTOR {
        return Err(EvmExecutionError::IllegalOperationCall(
            "function execute_message".to_string(),
        ));
    }
    if vec == PROCESS_STREAMS_SELECTOR {
        return Err(EvmExecutionError::IllegalOperationCall(
            "function process_streams".to_string(),
        ));
    }
    if vec == INSTANTIATE_SELECTOR {
        return Err(EvmExecutionError::IllegalOperationCall(
            "function instantiate".to_string(),
        ));
    }
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
    if !has_selector(module, selector) {
        return Err(EvmExecutionError::MissingFunction(fct_name.to_string()));
    }
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
    // See TODO(#3966) for a better support of the input.
    use alloy_primitives::{Bytes, B256};
    use alloy_sol_types::{sol, SolCall};
    use linera_base::identifiers::{GenericApplicationId, StreamId};
    sol! {
        struct InternalCryptoHash {
            bytes32 value;
        }

        struct InternalApplicationId {
            InternalCryptoHash application_description_hash;
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
            InternalCryptoHash value;
        }

        struct InternalStreamUpdate {
            InternalChainId chain_id;
            InternalStreamId stream_id;
            uint32 previous_index;
            uint32 next_index;
        }

        function process_streams(InternalStreamUpdate[] internal_streams);
    }

    fn crypto_hash_to_internal_crypto_hash(hash: CryptoHash) -> InternalCryptoHash {
        let hash: [u64; 4] = <[u64; 4]>::from(hash);
        let hash: [u8; 32] = linera_base::crypto::u64_array_to_be_bytes(hash);
        let value: B256 = hash.into();
        InternalCryptoHash { value }
    }

    fn chain_id_to_internal_chain_id(chain_id: ChainId) -> InternalChainId {
        let value = crypto_hash_to_internal_crypto_hash(chain_id.0);
        InternalChainId { value }
    }

    fn application_id_to_internal_application_id(
        application_id: ApplicationId,
    ) -> InternalApplicationId {
        let application_description_hash =
            crypto_hash_to_internal_crypto_hash(application_id.application_description_hash);
        InternalApplicationId {
            application_description_hash,
        }
    }

    fn stream_name_to_internal_stream_name(stream_name: StreamName) -> InternalStreamName {
        let stream_name = Bytes::from(stream_name.0);
        InternalStreamName { stream_name }
    }

    fn generic_application_id_to_internal_generic_application_id(
        generic_application_id: GenericApplicationId,
    ) -> InternalGenericApplicationId {
        match generic_application_id {
            GenericApplicationId::System => {
                let application_description_hash = InternalCryptoHash { value: B256::ZERO };
                InternalGenericApplicationId {
                    choice: 0,
                    user: InternalApplicationId {
                        application_description_hash,
                    },
                }
            }
            GenericApplicationId::User(application_id) => InternalGenericApplicationId {
                choice: 1,
                user: application_id_to_internal_application_id(application_id),
            },
        }
    }

    fn stream_id_to_internal_stream_id(stream_id: StreamId) -> InternalStreamId {
        let application_id =
            generic_application_id_to_internal_generic_application_id(stream_id.application_id);
        let stream_name = stream_name_to_internal_stream_name(stream_id.stream_name);
        InternalStreamId {
            application_id,
            stream_name,
        }
    }

    fn stream_update_to_internal_stream_update(
        stream_update: StreamUpdate,
    ) -> InternalStreamUpdate {
        let chain_id = chain_id_to_internal_chain_id(stream_update.chain_id);
        let stream_id = stream_id_to_internal_stream_id(stream_update.stream_id);
        InternalStreamUpdate {
            chain_id,
            stream_id,
            previous_index: stream_update.previous_index,
            next_index: stream_update.next_index,
        }
    }

    let internal_streams = streams
        .into_iter()
        .map(stream_update_to_internal_stream_update)
        .collect::<Vec<_>>();

    let fct_call = process_streamsCall { internal_streams };
    fct_call.abi_encode()
}

/// The instantiation argument to evm smart contracts.
/// value is the amount being transfered.
#[derive(Default, Serialize, Deserialize)]
pub struct EvmInstantiation {
    pub value: Amount,
    pub argument: Vec<u8>,
}

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct EvmMutation {
    pub value: U256,
    pub argument: Vec<u8>,
}

pub(crate) fn get_internal_mutation(value: U256, argument: Vec<u8>) -> Result<Vec<u8>, ExecutionError> {
    let evm_mutation = EvmMutation { value, argument };
    Ok(bcs::to_bytes(&evm_mutation)?)
}

pub fn get_mutation(amount: Amount, mutation: impl SolCall) -> Result<Vec<u8>, ExecutionError> {
    get_internal_mutation(amount.into(), mutation.abi_encode())
}

#[cfg(test)]
mod tests {
    use revm_primitives::keccak256;

    use crate::evm::inputs::{
        EXECUTE_MESSAGE_SELECTOR, INSTANTIATE_SELECTOR, PROCESS_STREAMS_SELECTOR,
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
        use alloy_sol_types::{sol, SolCall};
        sol! {
            struct InternalCryptoHash {
                bytes32 value;
            }

            struct InternalApplicationId {
                InternalCryptoHash application_description_hash;
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
                InternalCryptoHash value;
            }

            struct InternalStreamUpdate {
                InternalChainId chain_id;
                InternalStreamId stream_id;
                uint32 previous_index;
                uint32 next_index;
            }

            function process_streams(InternalStreamUpdate[] internal_streams);
        }
        assert_eq!(
            process_streamsCall::SIGNATURE,
            "process_streams((((bytes32)),((uint8,((bytes32))),(bytes)),uint32,uint32)[])"
        );
        assert_eq!(process_streamsCall::SELECTOR, PROCESS_STREAMS_SELECTOR);
    }

    #[test]
    fn check_instantiate_selector() {
        let selector = &keccak256("instantiate(bytes)".as_bytes())[..4];
        assert_eq!(selector, INSTANTIATE_SELECTOR);
    }
}
