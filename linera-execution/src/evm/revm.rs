// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Revm](https://bluealloy.github.io/revm/) runtime.

use core::ops::Range;
use std::{
    collections::BTreeSet,
    convert::TryFrom,
    sync::{Arc, Mutex},
};

#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    crypto::CryptoHash,
    data_types::{
        Amount, ApplicationDescription, Bytecode, Resources, SendMessageRequest, StreamUpdate,
    },
    ensure,
    identifiers::{Account, AccountOwner, ApplicationId, ChainId, ModuleId, StreamName},
    vm::{EvmInstantiation, EvmOperation, EvmQuery, VmRuntime},
};
use revm::{primitives::Bytes, InspectCommitEvm, InspectEvm, Inspector};
use revm_context::{
    result::{ExecutionResult, Output, SuccessReason},
    BlockEnv, Cfg, ContextTr, Evm, Journal, JournalTr, LocalContextTr as _, TxEnv,
};
use revm_database::WrapDatabaseRef;
use revm_handler::{
    instructions::EthInstructions, EthPrecompiles, MainnetContext, PrecompileProvider,
};
use revm_interpreter::{
    CallInput, CallInputs, CallOutcome, CallValue, CreateInputs, CreateOutcome, CreateScheme, Gas,
    InputsImpl, InstructionResult, InterpreterResult,
};
use revm_primitives::{hardfork::SpecId, Address, Log, TxKind, U256};
use revm_state::EvmState;
use serde::{Deserialize, Serialize};

use crate::{
    evm::{
        data_types::AmountU256,
        database::{DatabaseRuntime, StorageStats, EVM_SERVICE_GAS_LIMIT},
        inputs::{
            ensure_message_length, ensure_selector_presence, forbid_execute_operation_origin,
            get_revm_execute_message_bytes, get_revm_instantiation_bytes,
            get_revm_process_streams_bytes, has_selector, EXECUTE_MESSAGE_SELECTOR, FAUCET_ADDRESS,
            INSTANTIATE_SELECTOR, PRECOMPILE_ADDRESS, PROCESS_STREAMS_SELECTOR, SERVICE_ADDRESS,
            ZERO_ADDRESS,
        },
    },
    BaseRuntime, ContractRuntime, ContractSyncRuntimeHandle, DataBlobHash, EvmExecutionError,
    EvmRuntime, ExecutionError, ServiceRuntime, ServiceSyncRuntimeHandle, UserContract,
    UserContractInstance, UserContractModule, UserService, UserServiceInstance, UserServiceModule,
};

/// The selector when calling for `InterpreterResult`. This is a fictional
/// selector that does not correspond to a real function.
const INTERPRETER_RESULT_SELECTOR: &[u8] = &[1, 2, 3, 4];

/// The selector when accessing for the deployed bytecode. This is a fictional
/// selector that does not correspond to a real function.
const GET_DEPLOYED_BYTECODE_SELECTOR: &[u8] = &[21, 34, 55, 89];

/// The json serialization of a trivial vector.
const JSON_EMPTY_VECTOR: &[u8] = &[91, 93];

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

#[derive(Clone)]
pub enum EvmContractModule {
    #[cfg(with_revm)]
    Revm { module: Vec<u8> },
}

impl EvmContractModule {
    /// Creates a new [`EvmContractModule`] using the EVM module with the provided `contract_bytecode`.
    pub fn new(
        contract_bytecode: Bytecode,
        runtime: EvmRuntime,
    ) -> Result<Self, EvmExecutionError> {
        match runtime {
            #[cfg(with_revm)]
            EvmRuntime::Revm => Self::from_revm(contract_bytecode),
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
    }

    /// Creates a new [`EvmContractModule`] using Revm with the provided bytecode files.
    pub fn from_revm(contract_bytecode: Bytecode) -> Result<Self, EvmExecutionError> {
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
    pub fn new(service_bytecode: Bytecode, runtime: EvmRuntime) -> Result<Self, EvmExecutionError> {
        match runtime {
            #[cfg(with_revm)]
            EvmRuntime::Revm => Self::from_revm(service_bytecode),
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
    }

    /// Creates a new [`EvmServiceModule`] using Revm with the provided bytecode files.
    pub fn from_revm(contract_bytecode: Bytecode) -> Result<Self, EvmExecutionError> {
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
    /// Calling `authenticated_owner` of `ContractRuntime`
    AuthenticatedOwner,
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
    /// Calling `transfer` of `ContractRuntime`
    Transfer {
        account: Account,
        amount: AmountU256,
    },
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

fn get_precompile_output(output: Vec<u8>, gas_limit: u64) -> InterpreterResult {
    // The gas usage is set to `gas_limit` and no spending is being done on it.
    // This means that for Revm, it looks like the precompile call costs nothing.
    // This is because the costs of the EVM precompile calls is accounted for
    // separately in Linera.
    let output = Bytes::from(output);
    let result = InstructionResult::default();
    let gas = Gas::new(gas_limit);
    InterpreterResult {
        result,
        output,
        gas,
    }
}

fn get_argument<Ctx: ContextTr>(context: &mut Ctx, input: &CallInput) -> Vec<u8> {
    match input {
        CallInput::Bytes(bytes) => bytes.to_vec(),
        CallInput::SharedBuffer(range) => {
            match context.local().shared_memory_buffer_slice(range.clone()) {
                None => Vec::new(),
                Some(slice) => slice.to_vec(),
            }
        }
    }
}

fn get_value(call_value: &CallValue) -> Result<U256, EvmExecutionError> {
    match call_value {
        CallValue::Transfer(value) => Ok(*value),
        CallValue::Apparent(_) => Err(EvmExecutionError::NoDelegateCall),
    }
}

fn get_precompile_argument<Ctx: ContextTr>(
    context: &mut Ctx,
    inputs: &InputsImpl,
) -> Result<Vec<u8>, ExecutionError> {
    Ok(get_argument(context, &inputs.input))
}

fn get_call_service_argument<Ctx: ContextTr>(
    context: &mut Ctx,
    inputs: &CallInputs,
) -> Result<Vec<u8>, ExecutionError> {
    ensure!(
        get_value(&inputs.value)? == U256::ZERO,
        EvmExecutionError::NoTransferInServices
    );
    let mut argument = INTERPRETER_RESULT_SELECTOR.to_vec();
    argument.extend(&get_argument(context, &inputs.input));
    Ok(argument)
}

fn get_call_contract_argument<Ctx: ContextTr>(
    context: &mut Ctx,
    inputs: &CallInputs,
) -> Result<(Vec<u8>, usize), ExecutionError> {
    let mut final_argument = INTERPRETER_RESULT_SELECTOR.to_vec();
    let value = get_value(&inputs.value)?;
    let argument = get_argument(context, &inputs.input);
    let n_input = argument.len();
    let evm_operation = EvmOperation { value, argument };
    let argument = bcs::to_bytes(&evm_operation)?;
    final_argument.extend(&argument);
    Ok((final_argument, n_input))
}

fn base_runtime_call<Runtime: BaseRuntime>(
    request: BaseRuntimePrecompile,
    context: &mut Ctx<'_, Runtime>,
) -> Result<Vec<u8>, ExecutionError> {
    let mut runtime = context.db().0.runtime.lock().unwrap();
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
            let output = Self::call_or_fail(inputs, context)
                .map_err(|error| format!("ContractPrecompile error: {error}"))?;
            return Ok(Some(get_precompile_output(output, gas_limit)));
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

fn get_evm_destination<Runtime: ContractRuntime>(
    context: &mut Ctx<'_, Runtime>,
    account: Account,
) -> Result<Option<Address>, ExecutionError> {
    let mut runtime = context.db().0.runtime.lock().unwrap();
    if runtime.chain_id()? != account.chain_id {
        return Ok(None);
    }
    Ok(account.owner.to_evm_address())
}

/// We are doing transfers of value from a source to a destination.
fn revm_transfer<Runtime: ContractRuntime>(
    context: &mut Ctx<'_, Runtime>,
    source: Address,
    destination: Address,
    value: U256,
) -> Result<(), ExecutionError> {
    if let Some(error) = context.journal().transfer(source, destination, value)? {
        let error = format!("{error:?}");
        let error = EvmExecutionError::TransactError(error);
        return Err(error.into());
    }
    Ok(())
}

impl<'a> ContractPrecompile {
    fn contract_runtime_call<Runtime: ContractRuntime>(
        request: ContractRuntimePrecompile,
        context: &mut Ctx<'a, Runtime>,
    ) -> Result<Vec<u8>, ExecutionError> {
        match request {
            ContractRuntimePrecompile::AuthenticatedOwner => {
                let mut runtime = context.db().0.runtime.lock().unwrap();
                let account_owner = runtime.authenticated_owner()?;
                Ok(bcs::to_bytes(&account_owner)?)
            }

            ContractRuntimePrecompile::MessageOriginChainId => {
                let mut runtime = context.db().0.runtime.lock().unwrap();
                let origin_chain_id = runtime.message_origin_chain_id()?;
                Ok(bcs::to_bytes(&origin_chain_id)?)
            }

            ContractRuntimePrecompile::MessageIsBouncing => {
                let mut runtime = context.db().0.runtime.lock().unwrap();
                let result = runtime.message_is_bouncing()?;
                Ok(bcs::to_bytes(&result)?)
            }
            ContractRuntimePrecompile::AuthenticatedCallerId => {
                let mut runtime = context.db().0.runtime.lock().unwrap();
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
                let mut runtime = context.db().0.runtime.lock().unwrap();
                runtime.send_message(send_message_request)?;
                Ok(vec![])
            }
            ContractRuntimePrecompile::TryCallApplication { target, argument } => {
                let authenticated = true;
                let mut runtime = context.db().0.runtime.lock().unwrap();
                ensure!(
                    target != runtime.application_id()?,
                    EvmExecutionError::NoSelfCall
                );
                runtime.try_call_application(authenticated, target, argument)
            }
            ContractRuntimePrecompile::Emit { stream_name, value } => {
                let mut runtime = context.db().0.runtime.lock().unwrap();
                let result = runtime.emit(stream_name, value)?;
                Ok(bcs::to_bytes(&result)?)
            }
            ContractRuntimePrecompile::ReadEvent {
                chain_id,
                stream_name,
                index,
            } => {
                let mut runtime = context.db().0.runtime.lock().unwrap();
                runtime.read_event(chain_id, stream_name, index)
            }
            ContractRuntimePrecompile::SubscribeToEvents {
                chain_id,
                application_id,
                stream_name,
            } => {
                let mut runtime = context.db().0.runtime.lock().unwrap();
                runtime.subscribe_to_events(chain_id, application_id, stream_name)?;
                Ok(vec![])
            }
            ContractRuntimePrecompile::UnsubscribeFromEvents {
                chain_id,
                application_id,
                stream_name,
            } => {
                let mut runtime = context.db().0.runtime.lock().unwrap();
                runtime.unsubscribe_from_events(chain_id, application_id, stream_name)?;
                Ok(vec![])
            }
            ContractRuntimePrecompile::QueryService {
                application_id,
                query,
            } => {
                let mut runtime = context.db().0.runtime.lock().unwrap();
                ensure!(
                    application_id != runtime.application_id()?,
                    EvmExecutionError::NoSelfCall
                );
                runtime.query_service(application_id, query)
            }
            ContractRuntimePrecompile::ValidationRound => {
                let mut runtime = context.db().0.runtime.lock().unwrap();
                let value = runtime.validation_round()?;
                Ok(bcs::to_bytes(&value)?)
            }
            ContractRuntimePrecompile::Transfer { account, amount } => {
                if amount.0 != U256::ZERO {
                    let destination = {
                        let destination = get_evm_destination(context, account)?;
                        destination.unwrap_or(FAUCET_ADDRESS)
                    };
                    let application_id = {
                        let mut runtime = context.db().0.runtime.lock().unwrap();
                        let application_id = runtime.application_id()?;
                        let source = application_id.into();
                        let value = Amount::try_from(amount.0).map_err(EvmExecutionError::from)?;
                        runtime.transfer(source, account, value)?;
                        application_id
                    };
                    let source: Address = application_id.evm_address();
                    revm_transfer(context, source, destination, amount.0)?;
                }
                Ok(vec![])
            }
        }
    }

    fn call_or_fail<Runtime: ContractRuntime>(
        inputs: &InputsImpl,
        context: &mut Ctx<'a, Runtime>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let input = get_precompile_argument(context, inputs)?;
        match bcs::from_bytes(&input)? {
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
        let mut runtime = context.db().0.runtime.lock().unwrap();
        match request {
            ServiceRuntimePrecompile::TryQueryApplication { target, argument } => {
                ensure!(
                    target != runtime.application_id()?,
                    EvmExecutionError::NoSelfCall
                );
                runtime.try_query_application(target, argument)
            }
        }
    }

    fn call_or_fail<Runtime: ServiceRuntime>(
        inputs: &InputsImpl,
        context: &mut Ctx<'a, Runtime>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let input = get_precompile_argument(context, inputs)?;
        match bcs::from_bytes(&input)? {
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
            let output = Self::call_or_fail(inputs, context)
                .map_err(|error| format!("ServicePrecompile error: {error}"))?;
            return Ok(Some(get_precompile_output(output, gas_limit)));
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

fn map_result_create_outcome<Runtime: BaseRuntime>(
    database: &DatabaseRuntime<Runtime>,
    result: Result<Option<CreateOutcome>, ExecutionError>,
) -> Option<CreateOutcome> {
    match result {
        Err(error) => {
            database.insert_error(error);
            // The use of Revert immediately stops the execution.
            let result = InstructionResult::Revert;
            let output = Bytes::default();
            let gas = Gas::default();
            let result = InterpreterResult {
                result,
                output,
                gas,
            };
            Some(CreateOutcome {
                result,
                address: None,
            })
        }
        Ok(result) => result,
    }
}

fn map_result_call_outcome<Runtime: BaseRuntime>(
    database: &DatabaseRuntime<Runtime>,
    result: Result<Option<CallOutcome>, ExecutionError>,
) -> Option<CallOutcome> {
    match result {
        Err(error) => {
            database.insert_error(error);
            // The use of Revert immediately stops the execution.
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
    error: Arc<Mutex<Option<U256>>>,
}

impl<Runtime> Clone for CallInterceptorContract<Runtime> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            contract_address: self.contract_address,
            precompile_addresses: self.precompile_addresses.clone(),
            error: self.error.clone(),
        }
    }
}

impl<'a, Runtime: ContractRuntime> Inspector<Ctx<'a, Runtime>>
    for CallInterceptorContract<Runtime>
{
    fn create(
        &mut self,
        context: &mut Ctx<'a, Runtime>,
        inputs: &mut CreateInputs,
    ) -> Option<CreateOutcome> {
        let result = self.create_or_fail(context, inputs);
        map_result_create_outcome(&self.db, result)
    }

    fn call(
        &mut self,
        context: &mut Ctx<'a, Runtime>,
        inputs: &mut CallInputs,
    ) -> Option<CallOutcome> {
        let result = self.call_or_fail(context, inputs);
        map_result_call_outcome(&self.db, result)
    }
}

impl<Runtime: ContractRuntime> CallInterceptorContract<Runtime> {
    /// Gets the expected `ApplicationId`. We need to transfer
    /// native tokens before the application is created (see below).
    /// Therefore, we need to pre-compute the obtained application ID.
    fn get_expected_application_id(
        runtime: &mut Runtime,
        module_id: ModuleId,
    ) -> Result<ApplicationId, ExecutionError> {
        let chain_id = runtime.chain_id()?;
        let block_height = runtime.block_height()?;
        let application_index = runtime.peek_application_index()?;
        let parameters = JSON_EMPTY_VECTOR.to_vec(); // No constructor
        let required_application_ids = Vec::new();
        let application_description = ApplicationDescription {
            module_id,
            creator_chain_id: chain_id,
            block_height,
            application_index,
            parameters: parameters.clone(),
            required_application_ids,
        };
        Ok(ApplicationId::from(&application_description))
    }

    /// Publishes the `inputs`.
    fn publish_create_inputs(
        context: &mut Ctx<'_, Runtime>,
        inputs: &mut CreateInputs,
    ) -> Result<ModuleId, ExecutionError> {
        let contract = linera_base::data_types::Bytecode::new(inputs.init_code.to_vec());
        let service = linera_base::data_types::Bytecode::new(vec![]);
        let mut runtime = context.db().0.runtime.lock().unwrap();
        runtime.publish_module(contract, service, VmRuntime::Evm)
    }

    /// The function `fn create` of the inspector trait is called
    /// when a contract is going to be instantiated. Since the
    /// function can have some error case which are not supported
    /// in `fn create`, we call a `fn create_or_fail` that can
    /// return errors.
    /// When the database runtime is created, the EVM contract
    /// may or may not have been created. Therefore, at startup
    /// we have `is_revm_instantiated = false`. That boolean
    /// can be updated after `set_is_initialized`.
    ///
    /// The inspector can do two things:
    /// * It can change the inputs in `CreateInputs`. Here we
    ///   change the address being created.
    /// * It can return some specific CreateInput to be used.
    ///
    /// Therefore, the first case of the call is going to
    /// be about the creation of the contract with just the
    /// address being the one chosen by Linera.
    ///
    /// The second case occurs when the first contract has
    /// been created and that contract starts making new
    /// contracts.
    /// In relation to bytecode, the following notions are
    /// relevant:
    /// * The bytecode is created from the compilation.
    /// * The bytecode concatenated with the constructor
    ///   argument. This is what is sent to EVM when we
    ///   create a new contract.
    /// * The deployed bytecode. This is essentially the
    ///   bytecode minus the constructor code.
    ///
    /// In relation to that, the following points are
    /// important:
    /// * The inputs.init_code is formed by the concatenation
    ///   of compiled bytecode + constructor argument.
    /// * It is impossible to separate the compiled bytecode
    ///   from the constructor argument. Think for example
    ///   of the following two contracts:
    ///   constructor(uint a, uint b) {
    ///   value = a + b
    ///   }
    ///   or
    ///   constructor(uint b) {
    ///   value = 3 + b
    ///   }
    ///   Calling the first constructor with (3,4) leads
    ///   to the same concatenation as the second constructor
    ///   with input (4).
    /// * It turns out that we do not need to determine the
    ///   constructor argument.
    /// * What needs to be determined is the deployed bytecode.
    ///   This is stored in the AccountInfo entry. It is
    ///   the result of the execution by the Revm interpreter
    ///   and there is no way to do it without doing the execution.
    ///
    /// The strategy for creating the contract is thus:
    /// * For the case of a new contract being created, we proceed
    ///   like for services. We just adjust the address of the
    ///   creation.
    /// * In the second case, we first create the contract and
    ///   service bytecode (empty, but not used) and then publish
    ///   the module.
    /// * The parameters is empty because the constructor argument
    ///   have already put in the init_code.
    /// * The instantiation argument is empty since an EVM contract
    ///   creating a new contract will not support Linera features.
    ///   This is simply not part of create/create2 in the EVM.
    /// * That call to `create_application` leads to a creation of
    ///   a new contract and so a call to `fn create_or_fail` in
    ///   another instance of Revm.
    /// * When returning the `CreateOutcome`, we need to have the
    ///   deployed bytecode. This is implemented through a special
    ///   call to `GET_DEPLOYED_BYTECODE_SELECTOR`. This is done
    ///   with an `execute_operation`.
    /// * Data is put together as a `Some(...)` which tells Revm
    ///   that it does not need to execute the bytecode since the
    ///   output is given to it.
    ///
    /// The fact that the creation of a contract can be done in a
    /// parallel way to transferring native tokens to the contract
    /// being created requires us to handle this as well.
    /// * We cannot do a transfer from the calling contract with
    ///   `deposit_funds` because we would not have the right
    ///   to make the transfer.
    /// * Therefore, we have to do the transfer before
    ///   `create_application`. This forces us to know the
    ///   `application_id` before it is created. This is done
    ///   by building the `ApplicationDescription` and its hash.
    fn create_or_fail(
        &mut self,
        context: &mut Ctx<'_, Runtime>,
        inputs: &mut CreateInputs,
    ) -> Result<Option<CreateOutcome>, ExecutionError> {
        if !self.db.is_revm_instantiated {
            self.db.is_revm_instantiated = true;
            inputs.scheme = CreateScheme::Custom {
                address: self.contract_address,
            };
            Ok(None)
        } else {
            if inputs.value != U256::ZERO {
                // decrease the balance of the contract address by the expected amount.
                // We put the tokens in FAUCET_ADDRESS because we cannot transfer to
                // a contract that does not yet exist.
                // It is a common construction. We can see that in ERC20 contract code
                // for example for burning and minting.
                revm_transfer(
                    context,
                    self.db.contract_address,
                    FAUCET_ADDRESS,
                    inputs.value,
                )?;
            }
            let module_id = Self::publish_create_inputs(context, inputs)?;
            let (deployed_bytecode, address) = {
                let mut runtime = context.db().0.runtime.lock().unwrap();
                let chain_id = runtime.chain_id()?;
                let application_id = runtime.application_id()?;
                let expected_application_id =
                    Self::get_expected_application_id(&mut runtime, module_id)?;
                if inputs.value != U256::ZERO {
                    let amount = Amount::try_from(inputs.value).map_err(EvmExecutionError::from)?;
                    let destination = Account {
                        chain_id,
                        owner: expected_application_id.into(),
                    };
                    let source = application_id.into();
                    runtime.transfer(source, destination, amount)?;
                }
                let parameters = JSON_EMPTY_VECTOR.to_vec(); // No constructor
                let evm_call = EvmInstantiation {
                    value: inputs.value,
                    argument: Vec::new(),
                };
                let argument = serde_json::to_vec(&evm_call)?;
                let required_application_ids = Vec::new();
                let created_application_id = runtime.create_application(
                    module_id,
                    parameters,
                    argument,
                    required_application_ids,
                )?;
                assert_eq!(expected_application_id, created_application_id);
                let argument = GET_DEPLOYED_BYTECODE_SELECTOR.to_vec();
                let deployed_bytecode: Vec<u8> =
                    runtime.try_call_application(false, created_application_id, argument)?;
                let address = created_application_id.evm_address();
                (deployed_bytecode, address)
            };
            let bytecode = revm_state::Bytecode::new_legacy(deployed_bytecode.clone().into());
            context.journal().load_account(address)?;
            context.journal().set_code(address, bytecode);
            let result = InterpreterResult {
                result: InstructionResult::Return, // Only possibility if no error occurred.
                output: Bytes::from(deployed_bytecode),
                gas: Gas::new(inputs.gas_limit),
            };
            let creation_outcome = CreateOutcome {
                result,
                address: Some(address),
            };
            Ok(Some(creation_outcome))
        }
    }

    /// Every call to a contract passes by this function.
    /// Three kinds:
    /// --- Call to the EVM smart contract itself (the first call)
    /// --- Call to the PRECOMPILE smart contract.
    /// --- Call to other EVM smart contract
    ///
    /// The first case is handled by using Revm. This is
    /// when we call this contract.
    ///
    /// Calling the precompile is also handled by using Revm
    /// which would then use specific code for that purpose.
    ///
    /// Calling other EVM contracts is handled here and we have
    /// to produce `Some(_)` as output. Note that in the Evm
    /// transferring ethers is the same as calling a function.
    ///
    /// In Linera, transferring native tokens and calling a function are
    /// different operations. However, the block is accepted
    /// completely or not at all. Therefore, we can ensure the
    /// atomicity of the operations.
    fn call_or_fail(
        &mut self,
        context: &mut Ctx<'_, Runtime>,
        inputs: &mut CallInputs,
    ) -> Result<Option<CallOutcome>, ExecutionError> {
        if self.precompile_addresses.contains(&inputs.target_address)
            || inputs.target_address == self.contract_address
        {
            // Precompile calls are handled by the precompile code.
            // The EVM smart contract is being called
            return Ok(None);
        }
        // Handling the balances.
        if let CallValue::Transfer(value) = inputs.value {
            let source: AccountOwner = inputs.caller.into();
            let owner: AccountOwner = inputs.bytecode_address.into();
            if value != U256::ZERO {
                // In Linera, only non-zero transfers matter
                {
                    let mut runtime = context
                        .db()
                        .0
                        .runtime
                        .lock()
                        .expect("The lock should be possible");
                    let amount = Amount::try_from(value).map_err(EvmExecutionError::from)?;
                    let chain_id = runtime.chain_id()?;
                    let destination = Account { chain_id, owner };
                    runtime.transfer(source, destination, amount)?;
                }
                revm_transfer(context, inputs.caller, inputs.target_address, value)?;
            }
        }
        // Other smart contracts calls are handled by the runtime
        let target = address_to_user_application_id(inputs.target_address);
        let (argument, n_input) = get_call_contract_argument(context, inputs)?;
        let contract_call = {
            if n_input > 0 {
                // The input is non-empty. It is a contract call.
                true
            } else {
                // In case of empty input, we have two scenarios:
                // * We are calling an EVM application. In that case it has non-empty
                //   storage (at least the deployed code)
                // * It is a user account. In that case it has empty storage.
                let mut runtime = self.db.runtime.lock().unwrap();
                !runtime.has_empty_storage(target)?
            }
        };
        let result = if contract_call {
            let authenticated = true;
            let result = {
                let mut runtime = self.db.runtime.lock().unwrap();
                runtime.try_call_application(authenticated, target, argument)?
            };
            get_interpreter_result(&result, inputs)?
        } else {
            // User account, no call needed.
            InterpreterResult {
                result: InstructionResult::Stop,
                output: Bytes::default(),
                gas: Gas::new(inputs.gas_limit),
            }
        };
        let call_outcome = CallOutcome {
            result,
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
    /// See below on `fn create_or_fail`.
    fn create(
        &mut self,
        context: &mut Ctx<'a, Runtime>,
        inputs: &mut CreateInputs,
    ) -> Option<CreateOutcome> {
        let result = self.create_or_fail(context, inputs);
        map_result_create_outcome(&self.db, result)
    }

    /// See below on `fn call_or_fail`.
    fn call(
        &mut self,
        context: &mut Ctx<'a, Runtime>,
        inputs: &mut CallInputs,
    ) -> Option<CallOutcome> {
        let result = self.call_or_fail(context, inputs);
        map_result_call_outcome(&self.db, result)
    }
}

impl<Runtime: ServiceRuntime> CallInterceptorService<Runtime> {
    /// The function `fn create` of the inspector trait is called
    /// when a contract is going to be instantiated. Since the
    /// function can have some error case which are not supported
    /// in `fn create`, we call a `fn create_or_fail` that can
    /// return errors.
    /// When the database runtime is created, the EVM contract
    /// may or may not have been created. Therefore, at startup
    /// we have `is_revm_instantiated = false`. That boolean
    /// can be updated after `set_is_initialized`.
    ///
    /// The inspector can do two things:
    /// * It can change the inputs in `CreateInputs`. Here we
    ///   change the address being created.
    /// * It can return some specific CreateInput to be used.
    ///
    /// Therefore, the first case of the call is going to
    /// be about the creation of the contract with just the
    /// address being the one chosen by Linera.
    /// The second case of creating a new contract does not
    /// apply in services and so lead to an error.
    fn create_or_fail(
        &mut self,
        _context: &mut Ctx<'_, Runtime>,
        inputs: &mut CreateInputs,
    ) -> Result<Option<CreateOutcome>, ExecutionError> {
        if !self.db.is_revm_instantiated {
            self.db.is_revm_instantiated = true;
            inputs.scheme = CreateScheme::Custom {
                address: self.contract_address,
            };
            Ok(None)
        } else {
            Err(EvmExecutionError::NoContractCreationInService.into())
        }
    }

    /// Every call to a contract passes by this function.
    /// Three kinds:
    /// --- Call to the EVM smart contract itself
    /// --- Call to the PRECOMPILE smart contract.
    /// --- Call to other EVM smart contract
    ///
    /// The first kind is the call to the contract itself like
    /// constructor or from an external call.
    /// The second kind is precompile calls. This include the
    /// classic one but also the one that accesses the Linera
    /// functionalities.
    /// The last kind is the calls to other EVM smart contracts.
    fn call_or_fail(
        &mut self,
        context: &mut Ctx<'_, Runtime>,
        inputs: &mut CallInputs,
    ) -> Result<Option<CallOutcome>, ExecutionError> {
        if self.precompile_addresses.contains(&inputs.target_address)
            || inputs.target_address == self.contract_address
        {
            // Precompile calls are handled by the precompile code.
            // The EVM smart contract is being called
            return Ok(None);
        }
        // Other smart contracts calls are handled by the runtime
        let target = address_to_user_application_id(inputs.target_address);
        let argument = get_call_service_argument(context, inputs)?;
        let result = {
            let evm_query = EvmQuery::Query(argument);
            let evm_query = serde_json::to_vec(&evm_query)?;
            let mut runtime = self.db.runtime.lock().unwrap();
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
        let instantiation_argument = serde_json::from_slice::<EvmInstantiation>(&argument)?;
        self.initialize_contract(instantiation_argument.value, caller)?;
        if has_selector(&self.module, INSTANTIATE_SELECTOR) {
            let argument = get_revm_instantiation_bytes(instantiation_argument.argument);
            let result = self.transact_commit(EvmTxKind::Call, argument, U256::ZERO, caller)?;
            self.write_logs(result.logs, "instantiate")?;
        }
        Ok(())
    }

    fn execute_operation(&mut self, operation: Vec<u8>) -> Result<Vec<u8>, ExecutionError> {
        self.db.set_contract_address()?;
        ensure_message_length(operation.len(), 4)?;
        if operation == GET_DEPLOYED_BYTECODE_SELECTOR {
            return self.db.get_deployed_bytecode();
        }
        let caller = self.get_msg_address()?;
        let (gas_final, output, logs) = if &operation[..4] == INTERPRETER_RESULT_SELECTOR {
            ensure_message_length(operation.len(), 8)?;
            forbid_execute_operation_origin(&operation[4..8])?;
            let evm_call = bcs::from_bytes::<EvmOperation>(&operation[4..])?;
            let result = self.init_transact_commit(evm_call.argument, evm_call.value, caller)?;
            result.interpreter_result_and_logs()?
        } else {
            forbid_execute_operation_origin(&operation[..4])?;
            let evm_call = bcs::from_bytes::<EvmOperation>(&operation)?;
            let result = self.init_transact_commit(evm_call.argument, evm_call.value, caller)?;
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
        let value = U256::ZERO;
        self.execute_no_return_operation(operation, "message", value, caller)
    }

    fn process_streams(&mut self, streams: Vec<StreamUpdate>) -> Result<(), ExecutionError> {
        self.db.set_contract_address()?;
        let operation = get_revm_process_streams_bytes(streams);
        ensure_selector_presence(
            &self.module,
            PROCESS_STREAMS_SELECTOR,
            "function process_streams(Linera.StreamUpdate[] memory streams)",
        )?;
        // For process_streams, authenticated_owner and authenticated_called_id are None.
        let caller = Address::ZERO;
        let value = U256::ZERO;
        self.execute_no_return_operation(operation, "process_streams", value, caller)
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
        value: U256,
        caller: Address,
    ) -> Result<(), ExecutionError> {
        let result = self.init_transact_commit(operation, value, caller)?;
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
        value: U256,
        caller: Address,
    ) -> Result<ExecutionResultSuccess, ExecutionError> {
        // An application can be instantiated in Linera sense, but not in EVM sense,
        // that is the contract entries corresponding to the deployed contract may
        // be missing.
        if !self.db.set_is_initialized()? {
            self.initialize_contract(U256::ZERO, caller)?;
        }
        self.transact_commit(EvmTxKind::Call, vec, value, caller)
    }

    /// Initializes the contract.
    fn initialize_contract(&mut self, value: U256, caller: Address) -> Result<(), ExecutionError> {
        let mut vec_init = self.module.clone();
        let constructor_argument = self.db.constructor_argument()?;
        vec_init.extend_from_slice(&constructor_argument);
        let result = self.transact_commit(EvmTxKind::Create, vec_init, value, caller)?;
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
        let mut runtime = self.db.runtime.lock().unwrap();
        let application_id = runtime.authenticated_caller_id()?;
        if let Some(application_id) = application_id {
            return Ok(if application_id.is_evm() {
                application_id.evm_address()
            } else {
                Address::ZERO
            });
        };
        let account_owner = runtime.authenticated_owner()?;
        if let Some(AccountOwner::Address20(address)) = account_owner {
            return Ok(Address::from(address));
        };
        Ok(ZERO_ADDRESS)
    }

    fn transact_commit(
        &mut self,
        ch: EvmTxKind,
        input: Vec<u8>,
        value: U256,
        caller: Address,
    ) -> Result<ExecutionResultSuccess, ExecutionError> {
        self.db.caller = caller;
        self.db.value = value;
        self.db.deposit_funds()?;
        let data = Bytes::from(input);
        let kind = match ch {
            EvmTxKind::Create => TxKind::Create,
            EvmTxKind::Call => TxKind::Call(self.db.contract_address),
        };
        let inspector = CallInterceptorContract {
            db: self.db.clone(),
            contract_address: self.db.contract_address,
            precompile_addresses: precompile_addresses(),
            error: Arc::new(Mutex::new(None)),
        };
        let block_env = self.db.get_contract_block_env()?;
        let (max_size_evm_contract, gas_limit) = {
            let mut runtime = self.db.runtime.lock().unwrap();
            let gas_limit = runtime.remaining_fuel(VmRuntime::Evm)?;
            let max_size_evm_contract = runtime.maximum_blob_size()? as usize;
            (max_size_evm_contract, gas_limit)
        };
        let nonce = self.db.get_nonce(&caller)?;
        let result = {
            let mut ctx: revm_context::Context<
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
            ctx.cfg.limit_contract_code_size = Some(max_size_evm_contract);
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
                    value,
                    ..TxEnv::default()
                },
                inspector.clone(),
            )
            .map_err(|error| {
                let error = format!("{:?}", error);
                EvmExecutionError::TransactCommitError(error)
            })
        }?;
        self.db.process_any_error()?;
        let storage_stats = self.db.take_storage_stats();
        self.db.commit_changes()?;
        let result = process_execution_result(storage_stats, result)?;
        Ok(result)
    }

    fn consume_fuel(&mut self, gas_final: u64) -> Result<(), ExecutionError> {
        let mut runtime = self.db.runtime.lock().unwrap();
        runtime.consume_fuel(gas_final, VmRuntime::Evm)
    }

    fn write_logs(&mut self, logs: Vec<Log>, origin: &str) -> Result<(), ExecutionError> {
        // TODO(#3758): Extracting Ethereum events from the Linera events.
        if !logs.is_empty() {
            let mut runtime = self.db.runtime.lock().unwrap();
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
            EvmQuery::Operation(operation) => {
                let mut runtime = self.db.runtime.lock().unwrap();
                runtime.schedule_operation(operation)?;
                return Ok(Vec::new());
            }
            EvmQuery::Operations(operations) => {
                let mut runtime = self.db.runtime.lock().unwrap();
                for operation in operations {
                    runtime.schedule_operation(operation)?;
                }
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
        if !self.db.set_is_initialized()? {
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
        let caller = SERVICE_ADDRESS;
        let value = U256::ZERO;
        self.db.caller = caller;
        self.db.value = value;
        let data = Bytes::from(input);
        let block_env = self.db.get_service_block_env()?;
        let inspector = CallInterceptorService {
            db: self.db.clone(),
            contract_address: self.db.contract_address,
            precompile_addresses: precompile_addresses(),
        };
        let max_size_evm_contract = {
            let mut runtime = self.db.runtime.lock().unwrap();
            runtime.maximum_blob_size()? as usize
        };
        let nonce = self.db.get_nonce(&caller)?;
        let result_state = {
            let mut ctx: revm_context::Context<
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
            ctx.cfg.limit_contract_code_size = Some(max_size_evm_contract);
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
                    value,
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
        self.db.process_any_error()?;
        let storage_stats = self.db.take_storage_stats();
        Ok((
            process_execution_result(storage_stats, result_state.result)?,
            result_state.state,
        ))
    }
}
