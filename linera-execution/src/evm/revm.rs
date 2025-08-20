// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Revm](https://bluealloy.github.io/revm/) runtime.

use core::ops::Range;
use std::{collections::BTreeSet, convert::TryFrom};

#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Bytecode, Resources, SendMessageRequest, StreamUpdate},
    ensure,
    identifiers::{AccountOwner, ApplicationId, ChainId, StreamName},
    vm::{EvmQuery, VmRuntime},
};
use revm::{primitives::Bytes, InspectCommitEvm, InspectEvm, Inspector};
use revm_context::{
    result::{ExecutionResult, Output, SuccessReason},
    BlockEnv, Cfg, ContextTr, Evm, Journal, LocalContextTr, TxEnv,
};
use revm_database::WrapDatabaseRef;
use revm_handler::{
    instructions::EthInstructions, EthPrecompiles, MainnetContext, PrecompileProvider,
};
use revm_interpreter::{
    CallInput, CallInputs, CallOutcome, CreateInputs, CreateOutcome, CreateScheme, Gas, InputsImpl,
    InstructionResult, InterpreterResult,
};
use revm_primitives::{address, hardfork::SpecId, Address, Log, TxKind, U256};
use revm_state::EvmState;
use serde::{Deserialize, Serialize};

use crate::{
    evm::{
        data_types::AmountU256,
        database::{DatabaseRuntime, StorageStats, EVM_SERVICE_GAS_LIMIT},
    },
    BaseRuntime, ContractRuntime, ContractSyncRuntimeHandle, DataBlobHash, EvmExecutionError,
    EvmRuntime, ExecutionError, ServiceRuntime, ServiceSyncRuntimeHandle, UserContract,
    UserContractInstance, UserContractModule, UserService, UserServiceInstance, UserServiceModule,
};

/// This is the selector of the `execute_message` that should be called
/// only from a submitted message
const EXECUTE_MESSAGE_SELECTOR: &[u8] = &[173, 125, 234, 205];

/// This is the selector of the `process_streams` that should be called
/// only from a submitted message
const PROCESS_STREAMS_SELECTOR: &[u8] = &[254, 72, 102, 28];

/// This is the selector of the `instantiate` that should be called
/// only when creating a new instance of a shared contract
const INSTANTIATE_SELECTOR: &[u8] = &[156, 163, 60, 158];

fn forbid_execute_operation_origin(vec: &[u8]) -> Result<(), EvmExecutionError> {
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

fn ensure_message_length(actual_length: usize, min_length: usize) -> Result<(), EvmExecutionError> {
    ensure!(
        actual_length >= min_length,
        EvmExecutionError::OperationIsTooShort
    );
    Ok(())
}

fn ensure_selector_presence(
    module: &[u8],
    selector: &[u8],
    fct_name: &str,
) -> Result<(), EvmExecutionError> {
    if !has_selector(module, selector) {
        return Err(EvmExecutionError::MissingFunction(fct_name.to_string()));
    }
    Ok(())
}

/// The selector when calling for `InterpreterResult`. This is a fictional
/// selector that does not correspond to a real function.
const INTERPRETER_RESULT_SELECTOR: &[u8] = &[1, 2, 3, 4];

#[cfg(test)]
mod tests {
    use revm_primitives::keccak256;

    use crate::evm::revm::{
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

fn has_selector(module: &[u8], selector: &[u8]) -> bool {
    let push4 = 0x63; // An EVM instruction
    let mut vec = vec![push4];
    vec.extend(selector);
    module.windows(5).any(|window| window == vec)
}

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    pub static CONTRACT_INSTANTIATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "evm_contract_instantiation_latency",
            "EVM contract instantiation latency",
            &[],
            exponential_bucket_latencies(1.0),
        )
    });

    pub static SERVICE_INSTANTIATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "evm_service_instantiation_latency",
            "EVM service instantiation latency",
            &[],
            exponential_bucket_latencies(1.0),
        )
    });
}

fn get_revm_instantiation_bytes(value: Vec<u8>) -> Vec<u8> {
    use alloy_primitives::Bytes;
    use alloy_sol_types::{sol, SolCall};
    sol! {
        function instantiate(bytes value);
    }
    let bytes = Bytes::from(value);
    let argument = instantiateCall { value: bytes };
    argument.abi_encode()
}

fn get_revm_execute_message_bytes(value: Vec<u8>) -> Vec<u8> {
    use alloy_primitives::Bytes;
    use alloy_sol_types::{sol, SolCall};
    sol! {
        function execute_message(bytes value);
    }
    let value = Bytes::from(value);
    let argument = execute_messageCall { value };
    argument.abi_encode()
}

fn get_revm_process_streams_bytes(streams: Vec<StreamUpdate>) -> Vec<u8> {
    // See TODO(#3966) for a better support of the input.
    use alloy_primitives::{Bytes, B256};
    use alloy_sol_types::{sol, SolCall};
    use linera_base::identifiers::{GenericApplicationId, StreamId};
    sol! {
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
        let hash: [u64; 4] = <[u64; 4]>::from(hash);
        let hash: [u8; 32] = linera_base::crypto::u64_array_to_be_bytes(hash);
        hash.into()
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

#[derive(Clone)]
pub enum EvmContractModule {
    #[cfg(with_revm)]
    Revm { module: Vec<u8> },
}

impl EvmContractModule {
    /// Creates a new [`EvmContractModule`] using the EVM module with the provided `contract_bytecode`.
    pub async fn new(
        contract_bytecode: Bytecode,
        runtime: EvmRuntime,
    ) -> Result<Self, EvmExecutionError> {
        match runtime {
            #[cfg(with_revm)]
            EvmRuntime::Revm => Self::from_revm(contract_bytecode).await,
        }
    }

    /// Creates a new [`EvmContractModule`] using the EVM module in `contract_bytecode_file`.
    #[cfg(with_fs)]
    pub async fn from_file(
        contract_bytecode_file: impl AsRef<std::path::Path>,
        runtime: EvmRuntime,
    ) -> Result<Self, EvmExecutionError> {
        Self::new(
            Bytecode::load_from_file(contract_bytecode_file)
                .await
                .map_err(anyhow::Error::from)
                .map_err(EvmExecutionError::LoadContractModule)?,
            runtime,
        )
        .await
    }

    /// Creates a new [`EvmContractModule`] using Revm with the provided bytecode files.
    pub async fn from_revm(contract_bytecode: Bytecode) -> Result<Self, EvmExecutionError> {
        let module = contract_bytecode.bytes;
        Ok(EvmContractModule::Revm { module })
    }
}

impl UserContractModule for EvmContractModule {
    fn instantiate(
        &self,
        runtime: ContractSyncRuntimeHandle,
    ) -> Result<UserContractInstance, ExecutionError> {
        #[cfg(with_metrics)]
        let _instantiation_latency = metrics::CONTRACT_INSTANTIATION_LATENCY.measure_latency();

        let instance: UserContractInstance = match self {
            #[cfg(with_revm)]
            EvmContractModule::Revm { module } => {
                Box::new(RevmContractInstance::prepare(module.to_vec(), runtime))
            }
        };

        Ok(instance)
    }
}

/// A user service in a compiled EVM module.
#[derive(Clone)]
pub enum EvmServiceModule {
    #[cfg(with_revm)]
    Revm { module: Vec<u8> },
}

impl EvmServiceModule {
    /// Creates a new [`EvmServiceModule`] using the EVM module with the provided bytecode.
    pub async fn new(
        service_bytecode: Bytecode,
        runtime: EvmRuntime,
    ) -> Result<Self, EvmExecutionError> {
        match runtime {
            #[cfg(with_revm)]
            EvmRuntime::Revm => Self::from_revm(service_bytecode).await,
        }
    }

    /// Creates a new [`EvmServiceModule`] using the EVM module in `service_bytecode_file`.
    #[cfg(with_fs)]
    pub async fn from_file(
        service_bytecode_file: impl AsRef<std::path::Path>,
        runtime: EvmRuntime,
    ) -> Result<Self, EvmExecutionError> {
        Self::new(
            Bytecode::load_from_file(service_bytecode_file)
                .await
                .map_err(anyhow::Error::from)
                .map_err(EvmExecutionError::LoadServiceModule)?,
            runtime,
        )
        .await
    }

    /// Creates a new [`EvmServiceModule`] using Revm with the provided bytecode files.
    pub async fn from_revm(contract_bytecode: Bytecode) -> Result<Self, EvmExecutionError> {
        let module = contract_bytecode.bytes;
        Ok(EvmServiceModule::Revm { module })
    }
}

impl UserServiceModule for EvmServiceModule {
    fn instantiate(
        &self,
        runtime: ServiceSyncRuntimeHandle,
    ) -> Result<UserServiceInstance, ExecutionError> {
        #[cfg(with_metrics)]
        let _instantiation_latency = metrics::SERVICE_INSTANTIATION_LATENCY.measure_latency();

        let instance: UserServiceInstance = match self {
            #[cfg(with_revm)]
            EvmServiceModule::Revm { module } => {
                Box::new(RevmServiceInstance::prepare(module.to_vec(), runtime))
            }
        };

        Ok(instance)
    }
}

type Ctx<'a, Runtime> = MainnetContext<WrapDatabaseRef<&'a mut DatabaseRuntime<Runtime>>>;

// This is the precompile address that contains the Linera specific
// functionalities accessed from the EVM.
const PRECOMPILE_ADDRESS: Address = address!("000000000000000000000000000000000000000b");

// This is the zero address used when no address can be obtained from `authenticated_signer`
// and `authenticated_caller_id`. This scenario does not occur if an Address20 user calls or
// if an EVM contract calls another EVM contract.
const ZERO_ADDRESS: Address = address!("0000000000000000000000000000000000000000");

// This is the address being used for service calls.
const SERVICE_ADDRESS: Address = address!("0000000000000000000000000000000000002000");

fn address_to_user_application_id(address: Address) -> ApplicationId {
    let mut vec = vec![0_u8; 32];
    vec[..20].copy_from_slice(address.as_ref());
    ApplicationId::new(CryptoHash::try_from(&vec as &[u8]).unwrap())
}

/// Some functionalities from the BaseRuntime
#[derive(Debug, Serialize, Deserialize)]
enum BaseRuntimePrecompile {
    /// Calling `chain_id` of `BaseRuntime`
    ChainId,
    /// Calling `block_height_id` of `BaseRuntime`
    BlockHeight,
    /// Calling `application_creator_chain_id` of `BaseRuntime`
    ApplicationCreatorChainId,
    /// Calling `read_system_timestamp` of `BaseRuntime`
    ReadSystemTimestamp,
    /// Calling `read_chain_balance` of `BaseRuntime`
    ReadChainBalance,
    /// Calling `read_owner_balance` of `BaseRuntime`
    ReadOwnerBalance(AccountOwner),
    /// Calling `read_owner_balances` of `BaseRuntime`
    ReadOwnerBalances,
    /// Calling `read_balance_owners` of `BaseRuntime`
    ReadBalanceOwners,
    /// Calling `chain_ownership` of `BaseRuntime`
    ChainOwnership,
    /// Calling `read_data_blob` of `BaseRuntime`
    ReadDataBlob(DataBlobHash),
    /// Calling `assert_data_blob_exists` of `BaseRuntime`
    AssertDataBlobExists(DataBlobHash),
}

/// Some functionalities from the ContractRuntime not in BaseRuntime
#[derive(Debug, Serialize, Deserialize)]
enum ContractRuntimePrecompile {
    /// Calling `authenticated_signer` of `ContractRuntime`
    AuthenticatedSigner,
    /// Calling `message_origin_chain_id` of `ContractRuntime`
    MessageOriginChainId,
    /// Calling `message_is_bouncing` of `ContractRuntime`
    MessageIsBouncing,
    /// Calling `authenticated_caller_id` of `ContractRuntime`
    AuthenticatedCallerId,
    /// Calling `send_message` of `ContractRuntime`
    SendMessage {
        destination: ChainId,
        message: Vec<u8>,
    },
    /// Calling `try_call_application` of `ContractRuntime`
    TryCallApplication {
        target: ApplicationId,
        argument: Vec<u8>,
    },
    /// Calling `emit` of `ContractRuntime`
    Emit {
        stream_name: StreamName,
        value: Vec<u8>,
    },
    /// Calling `read_event` of `ContractRuntime`
    ReadEvent {
        chain_id: ChainId,
        stream_name: StreamName,
        index: u32,
    },
    /// Calling `subscribe_to_events` of `ContractRuntime`
    SubscribeToEvents {
        chain_id: ChainId,
        application_id: ApplicationId,
        stream_name: StreamName,
    },
    /// Calling `unsubscribe_from_events` of `ContractRuntime`
    UnsubscribeFromEvents {
        chain_id: ChainId,
        application_id: ApplicationId,
        stream_name: StreamName,
    },
    /// Calling `query_service` of `ContractRuntime`
    QueryService {
        application_id: ApplicationId,
        query: Vec<u8>,
    },
    /// Calling `validation_round` of `ContractRuntime`
    ValidationRound,
}

/// Some functionalities from the ServiceRuntime not in BaseRuntime
#[derive(Debug, Serialize, Deserialize)]
enum ServiceRuntimePrecompile {
    /// Calling `try_query_application` of `ServiceRuntime`
    TryQueryApplication {
        target: ApplicationId,
        argument: Vec<u8>,
    },
}

/// Key prefixes used to transmit precompiles.
#[derive(Debug, Serialize, Deserialize)]
enum RuntimePrecompile {
    Base(BaseRuntimePrecompile),
    Contract(ContractRuntimePrecompile),
    Service(ServiceRuntimePrecompile),
}

fn get_precompile_output(
    output: Vec<u8>,
    gas_limit: u64,
) -> Result<Option<InterpreterResult>, String> {
    // The gas usage is set to `gas_limit` and no spending is being done on it.
    // This means that for REVM, it looks like the precompile call costs nothing.
    // This is because the costs of the EVM precompile calls is accounted for
    // separately in Linera.
    let output = Bytes::from(output);
    let result = InstructionResult::default();
    let gas = Gas::new(gas_limit);
    Ok(Some(InterpreterResult {
        result,
        output,
        gas,
    }))
}

fn get_precompile_argument<Ctx: ContextTr>(context: &mut Ctx, input: &CallInput) -> Vec<u8> {
    let mut argument = Vec::new();
    get_argument(context, &mut argument, input);
    argument
}

fn base_runtime_call<Runtime: BaseRuntime>(
    request: BaseRuntimePrecompile,
    context: &mut Ctx<'_, Runtime>,
) -> Result<Vec<u8>, ExecutionError> {
    let mut runtime = context
        .db()
        .0
        .runtime
        .lock()
        .expect("The lock should be possible");
    match request {
        BaseRuntimePrecompile::ChainId => {
            let chain_id = runtime.chain_id()?;
            Ok(bcs::to_bytes(&chain_id)?)
        }
        BaseRuntimePrecompile::BlockHeight => {
            let block_height = runtime.block_height()?;
            Ok(bcs::to_bytes(&block_height)?)
        }
        BaseRuntimePrecompile::ApplicationCreatorChainId => {
            let chain_id = runtime.application_creator_chain_id()?;
            Ok(bcs::to_bytes(&chain_id)?)
        }
        BaseRuntimePrecompile::ReadSystemTimestamp => {
            let timestamp = runtime.read_system_timestamp()?;
            Ok(bcs::to_bytes(&timestamp)?)
        }
        BaseRuntimePrecompile::ReadChainBalance => {
            let balance: linera_base::data_types::Amount = runtime.read_chain_balance()?;
            let balance: AmountU256 = balance.into();
            Ok(bcs::to_bytes(&balance)?)
        }
        BaseRuntimePrecompile::ReadOwnerBalance(account_owner) => {
            let balance = runtime.read_owner_balance(account_owner)?;
            let balance = Into::<U256>::into(balance);
            Ok(bcs::to_bytes(&balance)?)
        }
        BaseRuntimePrecompile::ReadOwnerBalances => {
            let owner_balances = runtime.read_owner_balances()?;
            let owner_balances = owner_balances
                .into_iter()
                .map(|(account_owner, balance)| (account_owner, balance.into()))
                .collect::<Vec<(AccountOwner, AmountU256)>>();
            Ok(bcs::to_bytes(&owner_balances)?)
        }
        BaseRuntimePrecompile::ReadBalanceOwners => {
            let owners = runtime.read_balance_owners()?;
            Ok(bcs::to_bytes(&owners)?)
        }
        BaseRuntimePrecompile::ChainOwnership => {
            let chain_ownership = runtime.chain_ownership()?;
            Ok(bcs::to_bytes(&chain_ownership)?)
        }
        BaseRuntimePrecompile::ReadDataBlob(hash) => runtime.read_data_blob(hash),
        BaseRuntimePrecompile::AssertDataBlobExists(hash) => {
            runtime.assert_data_blob_exists(hash)?;
            Ok(Vec::new())
        }
    }
}

fn precompile_addresses() -> BTreeSet<Address> {
    let mut addresses = BTreeSet::new();
    for address in EthPrecompiles::default().warm_addresses() {
        addresses.insert(address);
    }
    addresses.insert(PRECOMPILE_ADDRESS);
    addresses
}

#[derive(Debug, Default)]
struct ContractPrecompile {
    inner: EthPrecompiles,
}

impl<'a, Runtime: ContractRuntime> PrecompileProvider<Ctx<'a, Runtime>> for ContractPrecompile {
    type Output = InterpreterResult;

    fn set_spec(&mut self, spec: <<Ctx<'a, Runtime> as ContextTr>::Cfg as Cfg>::Spec) -> bool {
        <EthPrecompiles as PrecompileProvider<Ctx<'a, Runtime>>>::set_spec(&mut self.inner, spec)
    }

    fn run(
        &mut self,
        context: &mut Ctx<'a, Runtime>,
        address: &Address,
        inputs: &InputsImpl,
        is_static: bool,
        gas_limit: u64,
    ) -> Result<Option<InterpreterResult>, String> {
        if address == &PRECOMPILE_ADDRESS {
            let input = get_precompile_argument(context, &inputs.input);
            let output = Self::call_or_fail(&input, context)
                .map_err(|error| format!("ContractPrecompile error: {error}"))?;
            return get_precompile_output(output, gas_limit);
        }
        self.inner
            .run(context, address, inputs, is_static, gas_limit)
    }

    fn warm_addresses(&self) -> Box<impl Iterator<Item = Address>> {
        let mut addresses = self.inner.warm_addresses().collect::<Vec<Address>>();
        addresses.push(PRECOMPILE_ADDRESS);
        Box::new(addresses.into_iter())
    }

    fn contains(&self, address: &Address) -> bool {
        address == &PRECOMPILE_ADDRESS || self.inner.contains(address)
    }
}

impl<'a> ContractPrecompile {
    fn contract_runtime_call<Runtime: ContractRuntime>(
        request: ContractRuntimePrecompile,
        context: &mut Ctx<'a, Runtime>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let mut runtime = context
            .db()
            .0
            .runtime
            .lock()
            .expect("The lock should be possible");
        match request {
            ContractRuntimePrecompile::AuthenticatedSigner => {
                let account_owner = runtime.authenticated_signer()?;
                Ok(bcs::to_bytes(&account_owner)?)
            }

            ContractRuntimePrecompile::MessageOriginChainId => {
                let origin_chain_id = runtime.message_origin_chain_id()?;
                Ok(bcs::to_bytes(&origin_chain_id)?)
            }

            ContractRuntimePrecompile::MessageIsBouncing => {
                let result = runtime.message_is_bouncing()?;
                Ok(bcs::to_bytes(&result)?)
            }
            ContractRuntimePrecompile::AuthenticatedCallerId => {
                let application_id = runtime.authenticated_caller_id()?;
                Ok(bcs::to_bytes(&application_id)?)
            }
            ContractRuntimePrecompile::SendMessage {
                destination,
                message,
            } => {
                let authenticated = true;
                let is_tracked = true;
                let grant = Resources::default();
                let send_message_request = SendMessageRequest {
                    destination,
                    authenticated,
                    is_tracked,
                    grant,
                    message,
                };
                runtime.send_message(send_message_request)?;
                Ok(vec![])
            }
            ContractRuntimePrecompile::TryCallApplication { target, argument } => {
                let authenticated = true;
                runtime.try_call_application(authenticated, target, argument)
            }
            ContractRuntimePrecompile::Emit { stream_name, value } => {
                let result = runtime.emit(stream_name, value)?;
                Ok(bcs::to_bytes(&result)?)
            }
            ContractRuntimePrecompile::ReadEvent {
                chain_id,
                stream_name,
                index,
            } => runtime.read_event(chain_id, stream_name, index),
            ContractRuntimePrecompile::SubscribeToEvents {
                chain_id,
                application_id,
                stream_name,
            } => {
                runtime.subscribe_to_events(chain_id, application_id, stream_name)?;
                Ok(vec![])
            }
            ContractRuntimePrecompile::UnsubscribeFromEvents {
                chain_id,
                application_id,
                stream_name,
            } => {
                runtime.unsubscribe_from_events(chain_id, application_id, stream_name)?;
                Ok(vec![])
            }
            ContractRuntimePrecompile::QueryService {
                application_id,
                query,
            } => runtime.query_service(application_id, query),
            ContractRuntimePrecompile::ValidationRound => {
                let value = runtime.validation_round()?;
                Ok(bcs::to_bytes(&value)?)
            }
        }
    }

    fn call_or_fail<Runtime: ContractRuntime>(
        input: &[u8],
        context: &mut Ctx<'a, Runtime>,
    ) -> Result<Vec<u8>, ExecutionError> {
        match bcs::from_bytes(input)? {
            RuntimePrecompile::Base(base_tag) => base_runtime_call(base_tag, context),
            RuntimePrecompile::Contract(contract_tag) => {
                Self::contract_runtime_call(contract_tag, context)
            }
            RuntimePrecompile::Service(_) => Err(EvmExecutionError::PrecompileError(
                "Service tags are not available in GeneralContractCall".to_string(),
            )
            .into()),
        }
    }
}

#[derive(Debug, Default)]
struct ServicePrecompile {
    inner: EthPrecompiles,
}

impl<'a> ServicePrecompile {
    fn service_runtime_call<Runtime: ServiceRuntime>(
        request: ServiceRuntimePrecompile,
        context: &mut Ctx<'a, Runtime>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let mut runtime = context
            .db()
            .0
            .runtime
            .lock()
            .expect("The lock should be possible");
        match request {
            ServiceRuntimePrecompile::TryQueryApplication { target, argument } => {
                runtime.try_query_application(target, argument)
            }
        }
    }

    fn call_or_fail<Runtime: ServiceRuntime>(
        input: &[u8],
        context: &mut Ctx<'a, Runtime>,
    ) -> Result<Vec<u8>, ExecutionError> {
        match bcs::from_bytes(input)? {
            RuntimePrecompile::Base(base_tag) => base_runtime_call(base_tag, context),
            RuntimePrecompile::Contract(_) => Err(EvmExecutionError::PrecompileError(
                "Contract calls are not available in GeneralServiceCall".to_string(),
            )
            .into()),
            RuntimePrecompile::Service(service_tag) => {
                Self::service_runtime_call(service_tag, context)
            }
        }
    }
}

impl<'a, Runtime: ServiceRuntime> PrecompileProvider<Ctx<'a, Runtime>> for ServicePrecompile {
    type Output = InterpreterResult;

    fn set_spec(&mut self, spec: <<Ctx<'a, Runtime> as ContextTr>::Cfg as Cfg>::Spec) -> bool {
        <EthPrecompiles as PrecompileProvider<Ctx<'a, Runtime>>>::set_spec(&mut self.inner, spec)
    }

    fn run(
        &mut self,
        context: &mut Ctx<'a, Runtime>,
        address: &Address,
        inputs: &InputsImpl,
        is_static: bool,
        gas_limit: u64,
    ) -> Result<Option<InterpreterResult>, String> {
        if address == &PRECOMPILE_ADDRESS {
            let input = get_precompile_argument(context, &inputs.input);
            let output = Self::call_or_fail(&input, context)
                .map_err(|error| format!("ServicePrecompile error: {error}"))?;
            return get_precompile_output(output, gas_limit);
        }
        self.inner
            .run(context, address, inputs, is_static, gas_limit)
    }

    fn warm_addresses(&self) -> Box<impl Iterator<Item = Address>> {
        let mut addresses = self.inner.warm_addresses().collect::<Vec<Address>>();
        addresses.push(PRECOMPILE_ADDRESS);
        Box::new(addresses.into_iter())
    }

    fn contains(&self, address: &Address) -> bool {
        address == &PRECOMPILE_ADDRESS || self.inner.contains(address)
    }
}

fn map_result_call_outcome(
    result: Result<Option<CallOutcome>, ExecutionError>,
) -> Option<CallOutcome> {
    match result {
        Err(_error) => {
            // An alternative way would be to return None, which would induce
            // Revm to call the smart contract in its database, where it is
            // non-existent.
            let result = InstructionResult::Revert;
            let output = Bytes::default();
            let gas = Gas::default();
            let result = InterpreterResult {
                result,
                output,
                gas,
            };
            let memory_offset = Range::default();
            Some(CallOutcome {
                result,
                memory_offset,
            })
        }
        Ok(result) => result,
    }
}

fn get_interpreter_result(
    result: &[u8],
    inputs: &mut CallInputs,
) -> Result<InterpreterResult, ExecutionError> {
    let mut result = bcs::from_bytes::<InterpreterResult>(result)?;
    // This effectively means that no cost is incurred by the call to another contract.
    // This is fine since the costs are incurred by the other contract itself.
    result.gas = Gas::new(inputs.gas_limit);
    Ok(result)
}

struct CallInterceptorContract<Runtime> {
    db: DatabaseRuntime<Runtime>,
    // This is the contract address of the contract being created.
    contract_address: Address,
    precompile_addresses: BTreeSet<Address>,
}

impl<Runtime> Clone for CallInterceptorContract<Runtime> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            contract_address: self.contract_address,
            precompile_addresses: self.precompile_addresses.clone(),
        }
    }
}

fn get_argument<Ctx: ContextTr>(context: &mut Ctx, argument: &mut Vec<u8>, input: &CallInput) {
    match input {
        CallInput::Bytes(bytes) => {
            argument.extend(bytes.to_vec());
        }
        CallInput::SharedBuffer(range) => {
            if let Some(slice) = context.local().shared_memory_buffer_slice(range.clone()) {
                argument.extend(&*slice);
            }
        }
    };
}

fn get_call_argument<Ctx: ContextTr>(context: &mut Ctx, inputs: &CallInputs) -> Vec<u8> {
    let mut argument = INTERPRETER_RESULT_SELECTOR.to_vec();
    get_argument(context, &mut argument, &inputs.input);
    argument
}

impl<'a, Runtime: ContractRuntime> Inspector<Ctx<'a, Runtime>>
    for CallInterceptorContract<Runtime>
{
    fn create(
        &mut self,
        _context: &mut Ctx<'a, Runtime>,
        inputs: &mut CreateInputs,
    ) -> Option<CreateOutcome> {
        inputs.scheme = CreateScheme::Custom {
            address: self.contract_address,
        };
        None
    }

    fn call(
        &mut self,
        context: &mut Ctx<'a, Runtime>,
        inputs: &mut CallInputs,
    ) -> Option<CallOutcome> {
        let result = self.call_or_fail(context, inputs);
        map_result_call_outcome(result)
    }
}

impl<Runtime: ContractRuntime> CallInterceptorContract<Runtime> {
    fn call_or_fail(
        &mut self,
        context: &mut Ctx<'_, Runtime>,
        inputs: &mut CallInputs,
    ) -> Result<Option<CallOutcome>, ExecutionError> {
        // Every call to a contract passes by this function.
        // Three kinds:
        // --- Call to the PRECOMPILE smart contract.
        // --- Call to the EVM smart contract itself
        // --- Call to other EVM smart contract
        if self.precompile_addresses.contains(&inputs.target_address)
            || inputs.target_address == self.contract_address
        {
            // Precompile calls are handled by the precompile code.
            // The EVM smart contract is being called
            return Ok(None);
        }
        // Other smart contracts calls are handled by the runtime
        let target = address_to_user_application_id(inputs.target_address);
        let argument = get_call_argument(context, inputs);
        let authenticated = true;
        let result = {
            let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
            runtime.try_call_application(authenticated, target, argument)?
        };
        let call_outcome = CallOutcome {
            result: get_interpreter_result(&result, inputs)?,
            memory_offset: inputs.return_memory_offset.clone(),
        };
        Ok(Some(call_outcome))
    }
}

struct CallInterceptorService<Runtime> {
    db: DatabaseRuntime<Runtime>,
    // This is the contract address of the contract being created.
    contract_address: Address,
    precompile_addresses: BTreeSet<Address>,
}

impl<Runtime> Clone for CallInterceptorService<Runtime> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            contract_address: self.contract_address,
            precompile_addresses: self.precompile_addresses.clone(),
        }
    }
}

impl<'a, Runtime: ServiceRuntime> Inspector<Ctx<'a, Runtime>> for CallInterceptorService<Runtime> {
    fn create(
        &mut self,
        _context: &mut Ctx<'a, Runtime>,
        inputs: &mut CreateInputs,
    ) -> Option<CreateOutcome> {
        inputs.scheme = CreateScheme::Custom {
            address: self.contract_address,
        };
        None
    }

    fn call(
        &mut self,
        context: &mut Ctx<'a, Runtime>,
        inputs: &mut CallInputs,
    ) -> Option<CallOutcome> {
        let result = self.call_or_fail(context, inputs);
        map_result_call_outcome(result)
    }
}

impl<Runtime: ServiceRuntime> CallInterceptorService<Runtime> {
    fn call_or_fail(
        &mut self,
        context: &mut Ctx<'_, Runtime>,
        inputs: &mut CallInputs,
    ) -> Result<Option<CallOutcome>, ExecutionError> {
        // Every call to a contract passes by this function.
        // Three kinds:
        // --- Call to the PRECOMPILE smart contract.
        // --- Call to the EVM smart contract itself
        // --- Call to other EVM smart contract
        if self.precompile_addresses.contains(&inputs.target_address)
            || inputs.target_address == self.contract_address
        {
            // Precompile calls are handled by the precompile code.
            // The EVM smart contract is being called
            return Ok(None);
        }
        // Other smart contracts calls are handled by the runtime
        let target = address_to_user_application_id(inputs.target_address);
        let argument = get_call_argument(context, inputs);
        let result = {
            let evm_query = EvmQuery::Query(argument);
            let evm_query = serde_json::to_vec(&evm_query)?;
            let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
            runtime.try_query_application(target, evm_query)?
        };
        let call_outcome = CallOutcome {
            result: get_interpreter_result(&result, inputs)?,
            memory_offset: inputs.return_memory_offset.clone(),
        };
        Ok(Some(call_outcome))
    }
}

pub struct RevmContractInstance<Runtime> {
    module: Vec<u8>,
    db: DatabaseRuntime<Runtime>,
}

#[derive(Debug)]
enum EvmTxKind {
    Create,
    Call,
}

#[derive(Debug)]
struct ExecutionResultSuccess {
    reason: SuccessReason,
    gas_final: u64,
    logs: Vec<Log>,
    output: Output,
}

impl ExecutionResultSuccess {
    fn interpreter_result_and_logs(self) -> Result<(u64, Vec<u8>, Vec<Log>), ExecutionError> {
        let result: InstructionResult = self.reason.into();
        let Output::Call(output) = self.output else {
            unreachable!("The output should have been created from a EvmTxKind::Call");
        };
        let gas = Gas::new(0);
        let result = InterpreterResult {
            result,
            output,
            gas,
        };
        let result = bcs::to_bytes(&result)?;
        Ok((self.gas_final, result, self.logs))
    }

    fn output_and_logs(self) -> (u64, Vec<u8>, Vec<Log>) {
        let Output::Call(output) = self.output else {
            unreachable!("The output should have been created from a EvmTxKind::Call");
        };
        let output = output.as_ref().to_vec();
        (self.gas_final, output, self.logs)
    }

    // Checks that the contract has been correctly instantiated
    fn check_contract_initialization(&self, expected_address: Address) -> Result<(), String> {
        // Checks that the output is the expected one.
        let Output::Create(_, contract_address) = self.output else {
            return Err("Input should be ExmTxKind::Create".to_string());
        };
        // Checks that the contract address exists.
        let contract_address = contract_address.ok_or("Deployment failed")?;
        // Checks that the created contract address is the one of the `ApplicationId`.
        if contract_address == expected_address {
            Ok(())
        } else {
            Err("Contract address is not the same as ApplicationId".to_string())
        }
    }
}

impl<Runtime> UserContract for RevmContractInstance<Runtime>
where
    Runtime: ContractRuntime,
{
    fn instantiate(&mut self, argument: Vec<u8>) -> Result<(), ExecutionError> {
        self.db.set_contract_address()?;
        let caller = self.get_msg_address()?;
        self.initialize_contract(caller)?;
        if has_selector(&self.module, INSTANTIATE_SELECTOR) {
            let instantiation_argument = serde_json::from_slice::<Vec<u8>>(&argument)?;
            let argument = get_revm_instantiation_bytes(instantiation_argument);
            let result = self.transact_commit(EvmTxKind::Call, argument, caller)?;
            self.write_logs(result.logs, "instantiate")?;
        }
        Ok(())
    }

    fn execute_operation(&mut self, operation: Vec<u8>) -> Result<Vec<u8>, ExecutionError> {
        self.db.set_contract_address()?;
        ensure_message_length(operation.len(), 4)?;
        let caller = self.get_msg_address()?;
        let (gas_final, output, logs) = if &operation[..4] == INTERPRETER_RESULT_SELECTOR {
            ensure_message_length(operation.len(), 8)?;
            forbid_execute_operation_origin(&operation[4..8])?;
            let result = self.init_transact_commit(operation[4..].to_vec(), caller)?;
            result.interpreter_result_and_logs()?
        } else {
            ensure_message_length(operation.len(), 4)?;
            forbid_execute_operation_origin(&operation[..4])?;
            let result = self.init_transact_commit(operation, caller)?;
            result.output_and_logs()
        };
        self.consume_fuel(gas_final)?;
        self.write_logs(logs, "operation")?;
        Ok(output)
    }

    fn execute_message(&mut self, message: Vec<u8>) -> Result<(), ExecutionError> {
        self.db.set_contract_address()?;
        ensure_selector_presence(
            &self.module,
            EXECUTE_MESSAGE_SELECTOR,
            "function execute_message(bytes)",
        )?;
        let operation = get_revm_execute_message_bytes(message);
        let caller = self.get_msg_address()?;
        self.execute_no_return_operation(operation, "message", caller)
    }

    fn process_streams(&mut self, streams: Vec<StreamUpdate>) -> Result<(), ExecutionError> {
        self.db.set_contract_address()?;
        let operation = get_revm_process_streams_bytes(streams);
        ensure_selector_presence(
            &self.module,
            PROCESS_STREAMS_SELECTOR,
            "function process_streams(Linera.StreamUpdate[] memory streams)",
        )?;
        // For process_streams, authenticated_signer and authenticated_called_id are None.
        let caller = Address::ZERO;
        self.execute_no_return_operation(operation, "process_streams", caller)
    }

    fn finalize(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }
}

fn process_execution_result(
    storage_stats: StorageStats,
    result: ExecutionResult,
) -> Result<ExecutionResultSuccess, EvmExecutionError> {
    match result {
        ExecutionResult::Success {
            reason,
            gas_used,
            gas_refunded,
            logs,
            output,
        } => {
            let mut gas_final = gas_used;
            gas_final -= storage_stats.storage_costs();
            assert_eq!(gas_refunded, storage_stats.storage_refund());
            if !matches!(reason, SuccessReason::Return) {
                Err(EvmExecutionError::NoReturnInterpreter {
                    reason,
                    gas_used,
                    gas_refunded,
                    logs,
                    output,
                })
            } else {
                Ok(ExecutionResultSuccess {
                    reason,
                    gas_final,
                    logs,
                    output,
                })
            }
        }
        ExecutionResult::Revert { gas_used, output } => {
            Err(EvmExecutionError::Revert { gas_used, output })
        }
        ExecutionResult::Halt { gas_used, reason } => {
            Err(EvmExecutionError::Halt { gas_used, reason })
        }
    }
}

impl<Runtime> RevmContractInstance<Runtime>
where
    Runtime: ContractRuntime,
{
    pub fn prepare(module: Vec<u8>, runtime: Runtime) -> Self {
        let db = DatabaseRuntime::new(runtime);
        Self { module, db }
    }

    fn execute_no_return_operation(
        &mut self,
        operation: Vec<u8>,
        origin: &str,
        caller: Address,
    ) -> Result<(), ExecutionError> {
        let result = self.init_transact_commit(operation, caller)?;
        let (gas_final, output, logs) = result.output_and_logs();
        self.consume_fuel(gas_final)?;
        self.write_logs(logs, origin)?;
        assert_eq!(output.len(), 0);
        Ok(())
    }

    /// Executes the transaction. If needed initializes the contract.
    fn init_transact_commit(
        &mut self,
        vec: Vec<u8>,
        caller: Address,
    ) -> Result<ExecutionResultSuccess, ExecutionError> {
        // An application can be instantiated in Linera sense, but not in EVM sense,
        // that is the contract entries corresponding to the deployed contract may
        // be missing.
        if !self.db.is_initialized()? {
            self.initialize_contract(caller)?;
        }
        self.transact_commit(EvmTxKind::Call, vec, caller)
    }

    /// Initializes the contract.
    fn initialize_contract(&mut self, caller: Address) -> Result<(), ExecutionError> {
        let mut vec_init = self.module.clone();
        let constructor_argument = self.db.constructor_argument()?;
        vec_init.extend_from_slice(&constructor_argument);
        let result = self.transact_commit(EvmTxKind::Create, vec_init, caller)?;
        result
            .check_contract_initialization(self.db.contract_address)
            .map_err(EvmExecutionError::IncorrectContractCreation)?;
        self.write_logs(result.logs, "deploy")
    }

    /// Computes the address used in the `msg.sender` variable.
    /// It is computed in the following way:
    /// * If a Wasm contract calls an EVM contract then it is `Address::ZERO`.
    /// * If an EVM contract calls an EVM contract it is the address of the contract.
    /// * If a user having an `AccountOwner::Address32` address calls an EVM contract
    ///   then it is `Address::ZERO`.
    /// * If a user having an `AccountOwner::Address20` address calls an EVM contract
    ///   then it is this address.
    ///
    /// By doing this we ensure that EVM smart contracts works in the same way as
    /// on the EVM and that users and contracts outside of that realm can still
    /// call EVM smart contracts.
    fn get_msg_address(&self) -> Result<Address, ExecutionError> {
        let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
        let application_id = runtime.authenticated_caller_id()?;
        if let Some(application_id) = application_id {
            return Ok(if application_id.is_evm() {
                application_id.evm_address()
            } else {
                Address::ZERO
            });
        };
        let account_owner = runtime.authenticated_signer()?;
        if let Some(AccountOwner::Address20(address)) = account_owner {
            return Ok(Address::from(address));
        };
        Ok(ZERO_ADDRESS)
    }

    fn transact_commit(
        &mut self,
        ch: EvmTxKind,
        input: Vec<u8>,
        caller: Address,
    ) -> Result<ExecutionResultSuccess, ExecutionError> {
        let data = Bytes::from(input);
        let kind = match ch {
            EvmTxKind::Create => TxKind::Create,
            EvmTxKind::Call => TxKind::Call(self.db.contract_address),
        };
        let inspector = CallInterceptorContract {
            db: self.db.clone(),
            contract_address: self.db.contract_address,
            precompile_addresses: precompile_addresses(),
        };
        let block_env = self.db.get_contract_block_env()?;
        let gas_limit = {
            let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
            runtime.remaining_fuel(VmRuntime::Evm)?
        };
        let nonce = self.db.get_nonce(&caller)?;
        let result = {
            let ctx: revm_context::Context<
                BlockEnv,
                _,
                _,
                _,
                Journal<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
                (),
            > = revm_context::Context::<BlockEnv, _, _, _, _, _>::new(
                WrapDatabaseRef(&mut self.db),
                SpecId::PRAGUE,
            )
            .with_block(block_env);
            let instructions = EthInstructions::new_mainnet();
            let mut evm = Evm::new_with_inspector(
                ctx,
                inspector.clone(),
                instructions,
                ContractPrecompile::default(),
            );
            evm.inspect_commit(
                TxEnv {
                    kind,
                    data,
                    nonce,
                    gas_limit,
                    caller,
                    ..TxEnv::default()
                },
                inspector,
            )
            .map_err(|error| {
                let error = format!("{:?}", error);
                EvmExecutionError::TransactCommitError(error)
            })
        }?;
        let storage_stats = self.db.take_storage_stats();
        self.db.commit_changes()?;
        let result = process_execution_result(storage_stats, result)?;
        Ok(result)
    }

    fn consume_fuel(&mut self, gas_final: u64) -> Result<(), ExecutionError> {
        let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
        runtime.consume_fuel(gas_final, VmRuntime::Evm)
    }

    fn write_logs(&mut self, logs: Vec<Log>, origin: &str) -> Result<(), ExecutionError> {
        // TODO(#3758): Extracting Ethereum events from the Linera events.
        if !logs.is_empty() {
            let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
            let block_height = runtime.block_height()?;
            let stream_name = bcs::to_bytes("ethereum_event")?;
            let stream_name = StreamName(stream_name);
            for log in &logs {
                let value = bcs::to_bytes(&(origin, block_height.0, log))?;
                runtime.emit(stream_name.clone(), value)?;
            }
        }
        Ok(())
    }
}

pub struct RevmServiceInstance<Runtime> {
    module: Vec<u8>,
    db: DatabaseRuntime<Runtime>,
}

impl<Runtime> RevmServiceInstance<Runtime>
where
    Runtime: ServiceRuntime,
{
    pub fn prepare(module: Vec<u8>, runtime: Runtime) -> Self {
        let db = DatabaseRuntime::new(runtime);
        Self { module, db }
    }
}

impl<Runtime> UserService for RevmServiceInstance<Runtime>
where
    Runtime: ServiceRuntime,
{
    fn handle_query(&mut self, argument: Vec<u8>) -> Result<Vec<u8>, ExecutionError> {
        self.db.set_contract_address()?;
        let evm_query = serde_json::from_slice(&argument)?;
        let query = match evm_query {
            EvmQuery::Query(vec) => vec,
            EvmQuery::Mutation(operation) => {
                let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
                runtime.schedule_operation(operation)?;
                return Ok(Vec::new());
            }
        };

        ensure_message_length(query.len(), 4)?;
        // We drop the logs since the "eth_call" execution does not return any log.
        // Also, for handle_query, we do not have associated costs.
        // More generally, there is gas costs associated to service operation.
        let answer = if &query[..4] == INTERPRETER_RESULT_SELECTOR {
            let result = self.init_transact(query[4..].to_vec())?;
            let (_gas_final, answer, _logs) = result.interpreter_result_and_logs()?;
            answer
        } else {
            let result = self.init_transact(query)?;
            let (_gas_final, output, _logs) = result.output_and_logs();
            serde_json::to_vec(&output)?
        };
        Ok(answer)
    }
}

impl<Runtime> RevmServiceInstance<Runtime>
where
    Runtime: ServiceRuntime,
{
    fn init_transact(&mut self, vec: Vec<u8>) -> Result<ExecutionResultSuccess, ExecutionError> {
        // In case of a shared application, we need to instantiate it first
        // However, since in ServiceRuntime, we cannot modify the storage,
        // therefore the compiled contract is saved in the changes.
        if !self.db.is_initialized()? {
            let changes = {
                let mut vec_init = self.module.clone();
                let constructor_argument = self.db.constructor_argument()?;
                vec_init.extend_from_slice(&constructor_argument);
                let (result, changes) = self.transact(TxKind::Create, vec_init)?;
                result
                    .check_contract_initialization(self.db.contract_address)
                    .map_err(EvmExecutionError::IncorrectContractCreation)?;
                changes
            };
            self.db.changes = changes;
        }
        ensure_message_length(vec.len(), 4)?;
        forbid_execute_operation_origin(&vec[..4])?;
        let kind = TxKind::Call(self.db.contract_address);
        let (execution_result, _) = self.transact(kind, vec)?;
        Ok(execution_result)
    }

    fn transact(
        &mut self,
        kind: TxKind,
        input: Vec<u8>,
    ) -> Result<(ExecutionResultSuccess, EvmState), ExecutionError> {
        let data = Bytes::from(input);
        let block_env = self.db.get_service_block_env()?;
        let inspector = CallInterceptorService {
            db: self.db.clone(),
            contract_address: self.db.contract_address,
            precompile_addresses: precompile_addresses(),
        };
        let caller = SERVICE_ADDRESS;
        let nonce = self.db.get_nonce(&caller)?;
        let result_state = {
            let ctx: revm_context::Context<
                BlockEnv,
                _,
                _,
                _,
                Journal<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
                (),
            > = revm_context::Context::<BlockEnv, _, _, _, _, _>::new(
                WrapDatabaseRef(&mut self.db),
                SpecId::PRAGUE,
            )
            .with_block(block_env);
            let instructions = EthInstructions::new_mainnet();
            let mut evm = Evm::new_with_inspector(
                ctx,
                inspector.clone(),
                instructions,
                ServicePrecompile::default(),
            );
            evm.inspect(
                TxEnv {
                    kind,
                    data,
                    nonce,
                    caller,
                    gas_limit: EVM_SERVICE_GAS_LIMIT,
                    ..TxEnv::default()
                },
                inspector,
            )
            .map_err(|error| {
                let error = format!("{:?}", error);
                EvmExecutionError::TransactCommitError(error)
            })
        }?;
        let storage_stats = self.db.take_storage_stats();
        Ok((
            process_execution_result(storage_stats, result_state.result)?,
            result_state.state,
        ))
    }
}
