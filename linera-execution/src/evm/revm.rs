// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Revm](https://bluealloy.github.io/revm/) runtime.

use core::ops::Range;
use std::{convert::TryFrom, sync::Arc};

use linera_base::{
    crypto::CryptoHash,
    data_types::{Bytecode, Resources, SendMessageRequest, StreamUpdate},
    ensure,
    identifiers::{ApplicationId, ChainId, StreamName},
    vm::{EvmQuery, VmRuntime},
};
use num_enum::TryFromPrimitive;
use revm::{
    db::WrapDatabaseRef, inspector_handle_register, primitives::Bytes, ContextPrecompile,
    ContextStatefulPrecompile, Evm, EvmContext, InnerEvmContext, Inspector,
};
use revm_interpreter::{CallInputs, CallOutcome, Gas, InstructionResult, InterpreterResult};
use revm_precompile::PrecompileResult;
use revm_primitives::{
    address, Address, EvmState, ExecutionResult, Log, Output, PrecompileErrors, PrecompileOutput,
    SuccessReason, TxKind,
};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{
        exponential_bucket_latencies, register_histogram_vec, MeasureLatency as _,
    },
    prometheus::HistogramVec,
    std::sync::LazyLock,
};

use crate::{
    evm::database::{DatabaseRuntime, StorageStats, EVM_SERVICE_GAS_LIMIT},
    ContractRuntime, ContractSyncRuntimeHandle, EvmExecutionError, EvmRuntime, ExecutionError,
    ServiceRuntime, ServiceSyncRuntimeHandle, UserContract, UserContractInstance,
    UserContractModule, UserService, UserServiceInstance, UserServiceModule,
};

/// This is the selector of the `execute_message` that should be called
/// only from a submitted message
const EXECUTE_MESSAGE_SELECTOR: &[u8] = &[173, 125, 234, 205];

/// This is the selector of the `instantiate` that should be called
/// only when creating a new instance of a shared contract
const INSTANTIATE_SELECTOR: &[u8] = &[156, 163, 60, 158];

fn forbid_execute_operation_origin(vec: &[u8]) -> Result<(), ExecutionError> {
    ensure!(
        vec != EXECUTE_MESSAGE_SELECTOR,
        ExecutionError::EvmError(EvmExecutionError::OperationCallExecuteMessage)
    );
    ensure!(
        vec != INSTANTIATE_SELECTOR,
        ExecutionError::EvmError(EvmExecutionError::OperationCallInstantiate)
    );
    Ok(())
}

fn ensure_message_length(actual_length: usize, min_length: usize) -> Result<(), ExecutionError> {
    ensure!(
        actual_length >= min_length,
        ExecutionError::EvmError(EvmExecutionError::OperationIsTooShort)
    );
    Ok(())
}

/// The selector when calling for `InterpreterResult`. This is a fictional
/// selector that does not correspond to a real function.
const INTERPRETER_RESULT_SELECTOR: &[u8] = &[1, 2, 3, 4];

#[cfg(test)]
mod tests {
    use revm_primitives::keccak256;

    use crate::evm::revm::{EXECUTE_MESSAGE_SELECTOR, INSTANTIATE_SELECTOR};

    // The function keccak256 is not const so we cannot build the execute_message
    // selector directly.
    #[test]
    fn check_execute_message_selector() {
        let selector = &keccak256("execute_message(bytes)".as_bytes())[..4];
        assert_eq!(selector, EXECUTE_MESSAGE_SELECTOR);
    }

    #[test]
    fn check_instantiate_selector() {
        let selector = &keccak256("instantiate(bytes)".as_bytes())[..4];
        assert_eq!(selector, INSTANTIATE_SELECTOR);
    }
}

#[cfg(with_metrics)]
static CONTRACT_INSTANTIATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "evm_contract_instantiation_latency",
        "EVM contract instantiation latency",
        &[],
        exponential_bucket_latencies(1.0),
    )
});

#[cfg(with_metrics)]
static SERVICE_INSTANTIATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "evm_service_instantiation_latency",
        "EVM service instantiation latency",
        &[],
        exponential_bucket_latencies(1.0),
    )
});

fn get_revm_instantiation_bytes(value: Vec<u8>) -> Vec<u8> {
    use alloy_primitives::Bytes;
    use alloy_sol_types::{sol, SolCall};
    sol! {
        function instantiate(bytes value);
    }
    let bytes = Bytes::copy_from_slice(&value);
    let argument = instantiateCall { value: bytes };
    argument.abi_encode()
}

fn get_revm_execute_message_bytes(value: Vec<u8>) -> Vec<u8> {
    use alloy_primitives::Bytes;
    use alloy_sol_types::{sol, SolCall};
    sol! {
        function execute_message(bytes value);
    }
    let bytes = Bytes::copy_from_slice(&value);
    let argument = execute_messageCall { value: bytes };
    argument.abi_encode()
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
        let _instantiation_latency = CONTRACT_INSTANTIATION_LATENCY.measure_latency();

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
        let _instantiation_latency = SERVICE_INSTANTIATION_LATENCY.measure_latency();

        let instance: UserServiceInstance = match self {
            #[cfg(with_revm)]
            EvmServiceModule::Revm { module } => {
                Box::new(RevmServiceInstance::prepare(module.to_vec(), runtime))
            }
        };

        Ok(instance)
    }
}

// This is the precompile address that contains the Linera specific
// functionalities accessed from the EVM.
const PRECOMPILE_ADDRESS: Address = address!("000000000000000000000000000000000000000b");

fn u8_slice_to_application_id(vec: &[u8]) -> ApplicationId {
    // In calls the length is 32, so no problem unwrapping
    let hash = CryptoHash::try_from(vec).unwrap();
    ApplicationId::new(hash)
}

fn address_to_user_application_id(address: Address) -> ApplicationId {
    let mut vec = vec![0_u8; 32];
    vec[..20].copy_from_slice(address.as_ref());
    ApplicationId::new(CryptoHash::try_from(&vec as &[u8]).unwrap())
}

#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
enum PrecompileTag {
    /// Key prefix for `try_call_application`
    TryCallApplication,
    /// Key prefix for `try_query_application`
    TryQueryApplication,
    /// Key prefix for `send_message`
    SendMessage,
    /// Key prefix for `message_id`
    MessageId,
    /// Key prefix for `message_is_bouncing`
    MessageIsBouncing,
}

fn get_precompile_output(result: Vec<u8>) -> PrecompileOutput {
    // The gas usage is set to zero since the proper accounting is done
    // by the called application
    let gas_used = 0;
    let bytes = Bytes::copy_from_slice(&result);
    PrecompileOutput { gas_used, bytes }
}

struct GeneralContractCall;

impl<Runtime: ContractRuntime>
    ContextStatefulPrecompile<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>
    for GeneralContractCall
{
    fn call(
        &self,
        input: &Bytes,
        gas_limit: u64,
        context: &mut InnerEvmContext<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
    ) -> PrecompileResult {
        self.call_or_fail(input, gas_limit, context)
            .map_err(|msg| PrecompileErrors::Fatal { msg })
    }
}

const MESSAGE_IS_BOUNCING_NONE: u8 = 0;
const MESSAGE_IS_BOUNCING_SOME_TRUE: u8 = 1;
const MESSAGE_IS_BOUNCING_SOME_FALSE: u8 = 2;

impl GeneralContractCall {
    fn call_or_fail<Runtime: ContractRuntime>(
        &self,
        input: &Bytes,
        _gas_limit: u64,
        context: &mut InnerEvmContext<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
    ) -> Result<PrecompileOutput, String> {
        let vec = input.to_vec();
        ensure!(!vec.is_empty(), format!("vec.size() should be at least 1"));
        let tag = vec[0];
        let tag = PrecompileTag::try_from(tag)
            .map_err(|error| format!("{error} when trying to convert tag={tag}"))?;
        let result = {
            let mut runtime = context
                .db
                .0
                .runtime
                .lock()
                .expect("The lock should be possible");
            match tag {
                PrecompileTag::TryCallApplication => {
                    ensure!(vec.len() >= 33, format!("vec.size() should be at least 33"));
                    let target = u8_slice_to_application_id(&vec[1..33]);
                    let argument = vec[33..].to_vec();
                    let authenticated = true;
                    runtime
                        .try_call_application(authenticated, target, argument)
                        .map_err(|error| format!("TryCallApplication error: {error}"))
                }
                PrecompileTag::SendMessage => {
                    ensure!(vec.len() >= 33, format!("vec.size() should be at least 33"));
                    let destination = ChainId(
                        CryptoHash::try_from(&vec[1..33])
                            .map_err(|error| format!("TryError: {error}"))?,
                    );
                    let authenticated = true;
                    let is_tracked = true;
                    let grant = Resources::default();
                    let message = vec[33..].to_vec();
                    let send_message_request = SendMessageRequest {
                        destination,
                        authenticated,
                        is_tracked,
                        grant,
                        message,
                    };
                    runtime
                        .send_message(send_message_request)
                        .map_err(|error| format!("SendMessage error: {error}"))?;
                    Ok(vec![])
                }
                PrecompileTag::MessageId => {
                    ensure!(vec.len() == 1, format!("vec.size() should be exactly 1"));
                    let message_id = runtime
                        .message_id()
                        .map_err(|error| format!("MessageId error {error}"))?;
                    bcs::to_bytes(&message_id)
                        .map_err(|error| format!("MessageId serialization error {error}"))
                }
                PrecompileTag::MessageIsBouncing => {
                    ensure!(vec.len() == 1, format!("vec.size() should be exactly 1"));
                    let message_is_bouncing = runtime
                        .message_is_bouncing()
                        .map_err(|error| format!("MessageIsBouncing error {error}"))?;
                    let value = match message_is_bouncing {
                        None => MESSAGE_IS_BOUNCING_NONE,
                        Some(true) => MESSAGE_IS_BOUNCING_SOME_TRUE,
                        Some(false) => MESSAGE_IS_BOUNCING_SOME_FALSE,
                    };
                    Ok(vec![value])
                }
                _ => Err(format!("{tag:?} is not available in GeneralContractCall")),
            }
        }?;
        Ok(get_precompile_output(result))
    }
}

struct GeneralServiceCall;

impl<Runtime: ServiceRuntime>
    ContextStatefulPrecompile<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>
    for GeneralServiceCall
{
    fn call(
        &self,
        input: &Bytes,
        gas_limit: u64,
        context: &mut InnerEvmContext<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
    ) -> PrecompileResult {
        self.call_or_fail(input, gas_limit, context)
            .map_err(|msg| PrecompileErrors::Fatal { msg })
    }
}

impl GeneralServiceCall {
    fn call_or_fail<Runtime: ServiceRuntime>(
        &self,
        input: &Bytes,
        _gas_limit: u64,
        context: &mut InnerEvmContext<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
    ) -> Result<PrecompileOutput, String> {
        let vec = input.to_vec();
        ensure!(!vec.is_empty(), format!("vec.size() should be at least 1"));
        let tag = vec[0];
        let tag = PrecompileTag::try_from(tag)
            .map_err(|error| format!("{error} when trying to convert tag={tag}"))?;
        match tag {
            PrecompileTag::TryQueryApplication => {
                ensure!(vec.len() >= 33, format!("vec.size() should be at least 33"));
                let target = u8_slice_to_application_id(&vec[1..33]);
                let argument = vec[33..].to_vec();
                let result = {
                    let mut runtime = context
                        .db
                        .0
                        .runtime
                        .lock()
                        .expect("The lock should be possible");
                    runtime.try_query_application(target, argument)
                }
                .map_err(|error| format!("{}", error))?;
                Ok(get_precompile_output(result))
            }
            _ => Err(format!("{tag:?} is not available in GeneralServiceCall")),
        }
    }
}

fn failing_outcome() -> CallOutcome {
    let result = InstructionResult::Revert;
    let output = Bytes::default();
    let gas = Gas::default();
    let result = InterpreterResult {
        result,
        output,
        gas,
    };
    let memory_offset = Range::default();
    CallOutcome {
        result,
        memory_offset,
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
}

impl<Runtime: ContractRuntime> Inspector<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>
    for CallInterceptorContract<Runtime>
{
    fn call(
        &mut self,
        context: &mut EvmContext<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
        inputs: &mut CallInputs,
    ) -> Option<CallOutcome> {
        let result = self.call_or_fail(context, inputs);
        match result {
            Err(_error) => {
                // An alternative way would be to return None, which would induce
                // Revm to call the smart contract in its database, where it is
                // non-existent.
                Some(failing_outcome())
            }
            Ok(result) => result,
        }
    }
}

impl<Runtime: ContractRuntime> CallInterceptorContract<Runtime> {
    fn call_or_fail(
        &mut self,
        _context: &mut EvmContext<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
        inputs: &mut CallInputs,
    ) -> Result<Option<CallOutcome>, ExecutionError> {
        let contract_address = Address::ZERO.create(0);
        if inputs.target_address == PRECOMPILE_ADDRESS || inputs.target_address == contract_address
        {
            return Ok(None);
        }
        let vec = inputs.input.to_vec();
        let target = address_to_user_application_id(inputs.target_address);
        let mut argument: Vec<u8> = INTERPRETER_RESULT_SELECTOR.to_vec();
        argument.extend(&vec);
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
}

impl<Runtime: ServiceRuntime> Inspector<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>
    for CallInterceptorService<Runtime>
{
    fn call(
        &mut self,
        context: &mut EvmContext<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
        inputs: &mut CallInputs,
    ) -> Option<CallOutcome> {
        let result = self.call_or_fail(context, inputs);
        match result {
            Err(_error) => {
                // An alternative way would be to return None, which would induce
                // Revm to call the smart contract in its database, where it is
                // non-existent.
                Some(failing_outcome())
            }
            Ok(result) => result,
        }
    }
}

impl<Runtime: ServiceRuntime> CallInterceptorService<Runtime> {
    fn call_or_fail(
        &mut self,
        _context: &mut EvmContext<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
        inputs: &mut CallInputs,
    ) -> Result<Option<CallOutcome>, ExecutionError> {
        let contract_address = Address::ZERO.create(0);
        if inputs.target_address == PRECOMPILE_ADDRESS || inputs.target_address == contract_address
        {
            return Ok(None);
        }
        let vec = inputs.input.to_vec();
        let target = address_to_user_application_id(inputs.target_address);
        let mut argument: Vec<u8> = INTERPRETER_RESULT_SELECTOR.to_vec();
        argument.extend(&vec);
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

enum Choice {
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
            unreachable!("The Output is not a call which is impossible");
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
            unreachable!("It is impossible for a Choice::Call to lead to an Output::Create");
        };
        let output = output.as_ref().to_vec();
        (self.gas_final, output, self.logs)
    }
}

impl<Runtime> UserContract for RevmContractInstance<Runtime>
where
    Runtime: ContractRuntime,
{
    fn instantiate(&mut self, argument: Vec<u8>) -> Result<(), ExecutionError> {
        self.initialize_contract()?;
        let instantiation_argument = serde_json::from_slice::<Vec<u8>>(&argument)?;
        if !instantiation_argument.is_empty() {
            let argument = get_revm_instantiation_bytes(instantiation_argument);
            let result = self.transact_commit(Choice::Call, &argument)?;
            self.write_logs(result.logs, "instantiate")?;
        }
        Ok(())
    }

    fn execute_operation(&mut self, operation: Vec<u8>) -> Result<Vec<u8>, ExecutionError> {
        ensure_message_length(operation.len(), 4)?;
        let (gas_final, output, logs) = if &operation[..4] == INTERPRETER_RESULT_SELECTOR {
            ensure_message_length(operation.len(), 8)?;
            forbid_execute_operation_origin(&operation[4..8])?;
            let result = self.init_transact_commit(Choice::Call, &operation[4..])?;
            result.interpreter_result_and_logs()?
        } else {
            ensure_message_length(operation.len(), 4)?;
            forbid_execute_operation_origin(&operation[..4])?;
            let result = self.init_transact_commit(Choice::Call, &operation)?;
            result.output_and_logs()
        };
        self.consume_fuel(gas_final)?;
        self.write_logs(logs, "operation")?;
        Ok(output)
    }

    fn execute_message(&mut self, message: Vec<u8>) -> Result<(), ExecutionError> {
        let operation = get_revm_execute_message_bytes(message);
        let result = self.init_transact_commit(Choice::Call, &operation)?;
        let (gas_final, output, logs) = result.output_and_logs();
        self.consume_fuel(gas_final)?;
        self.write_logs(logs, "message")?;
        assert_eq!(output.len(), 0);
        Ok(())
    }

    fn process_streams(&mut self, _streams: Vec<StreamUpdate>) -> Result<(), ExecutionError> {
        // TODO(#3785): Implement process_streams for EVM
        todo!("Streams are not implemented for Ethereum smart contracts yet.")
    }

    fn finalize(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }
}

fn process_execution_result(
    storage_stats: StorageStats,
    result: ExecutionResult,
) -> Result<ExecutionResultSuccess, ExecutionError> {
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
            Ok(ExecutionResultSuccess {
                reason,
                gas_final,
                logs,
                output,
            })
        }
        ExecutionResult::Revert { gas_used, output } => {
            let error = EvmExecutionError::Revert { gas_used, output };
            Err(ExecutionError::EvmError(error))
        }
        ExecutionResult::Halt { gas_used, reason } => {
            let error = EvmExecutionError::Halt { gas_used, reason };
            Err(ExecutionError::EvmError(error))
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

    /// Executes the transaction. If needed initializes the contract.
    fn init_transact_commit(
        &mut self,
        ch: Choice,
        vec: &[u8],
    ) -> Result<ExecutionResultSuccess, ExecutionError> {
        // An application can be instantiated in Linera sense, but not in EVM sense,
        // that is the contract entries corresponding to the deployed contract may
        // be missing.
        if !self.db.is_initialized()? {
            self.initialize_contract()?;
        }
        self.transact_commit(ch, vec)
    }

    /// Initializes the contract.
    fn initialize_contract(&mut self) -> Result<(), ExecutionError> {
        let mut vec_init = self.module.clone();
        let constructor_argument = self.db.constructor_argument()?;
        vec_init.extend_from_slice(&constructor_argument);
        let result = self.transact_commit(Choice::Create, &vec_init)?;
        self.write_logs(result.logs, "deploy")
    }

    fn transact_commit(
        &mut self,
        ch: Choice,
        input: &[u8],
    ) -> Result<ExecutionResultSuccess, ExecutionError> {
        let (kind, tx_data) = match ch {
            Choice::Create => (TxKind::Create, Bytes::copy_from_slice(input)),
            Choice::Call => {
                let tx_data = Bytes::copy_from_slice(input);
                (TxKind::Call(Address::ZERO.create(0)), tx_data)
            }
        };
        let mut inspector = CallInterceptorContract {
            db: self.db.clone(),
        };
        let block_env = self.db.get_contract_block_env()?;
        let gas_limit = {
            let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
            runtime.remaining_fuel(VmRuntime::Evm)?
        };
        let result = {
            let mut evm: Evm<'_, _, _> = Evm::builder()
                .with_ref_db(&mut self.db)
                .with_external_context(&mut inspector)
                .modify_tx_env(|tx| {
                    tx.clear();
                    tx.transact_to = kind;
                    tx.data = tx_data;
                    tx.gas_limit = gas_limit;
                })
                .modify_block_env(|block| {
                    *block = block_env;
                })
                .append_handler_register(|handler| {
                    inspector_handle_register(handler);
                    let precompiles = handler.pre_execution.load_precompiles();
                    handler.pre_execution.load_precompiles = Arc::new(move || {
                        let mut precompiles = precompiles.clone();
                        precompiles.extend([(
                            PRECOMPILE_ADDRESS,
                            ContextPrecompile::ContextStateful(Arc::new(GeneralContractCall)),
                        )]);
                        precompiles
                    });
                })
                .build();

            evm.transact_commit().map_err(|error| {
                let error = format!("{:?}", error);
                let error = EvmExecutionError::TransactCommitError(error);
                ExecutionError::EvmError(error)
            })
        }?;
        let storage_stats = self.db.take_storage_stats();
        self.db.commit_changes()?;
        process_execution_result(storage_stats, result)
    }

    fn consume_fuel(&mut self, gas_final: u64) -> Result<(), ExecutionError> {
        let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
        runtime.consume_fuel(gas_final, VmRuntime::Evm)
    }

    fn write_logs(&mut self, logs: Vec<Log>, origin: &str) -> Result<(), ExecutionError> {
        // TODO(#3758): Extracting Ethereum events from the Linera events.
        if !logs.is_empty() {
            let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
            let stream_name = bcs::to_bytes("ethereum_event")?;
            let stream_name = StreamName(stream_name);
            for log in &logs {
                let value = bcs::to_bytes(&(origin, log))?;
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
            let result = self.init_transact(&query[4..])?;
            let (_gas_final, answer, _logs) = result.interpreter_result_and_logs()?;
            answer
        } else {
            let result = self.init_transact(&query)?;
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
    fn init_transact(&mut self, vec: &[u8]) -> Result<ExecutionResultSuccess, ExecutionError> {
        // In case of a shared application, we need to instantiate it first
        // However, since in ServiceRuntime, we cannot modify the storage,
        // therefore the compiled contract is saved in the changes.
        if !self.db.is_initialized()? {
            let changes = {
                let mut vec_init = self.module.clone();
                let constructor_argument = self.db.constructor_argument()?;
                vec_init.extend_from_slice(&constructor_argument);
                let kind = TxKind::Create;
                let (_, changes) = self.transact(kind, &vec_init)?;
                changes
            };
            self.db.changes = changes;
        }
        ensure_message_length(vec.len(), 4)?;
        forbid_execute_operation_origin(&vec[..4])?;
        let contract_address = Address::ZERO.create(0);
        let kind = TxKind::Call(contract_address);
        let (execution_result, _) = self.transact(kind, vec)?;
        Ok(execution_result)
    }

    fn transact(
        &mut self,
        kind: TxKind,
        input: &[u8],
    ) -> Result<(ExecutionResultSuccess, EvmState), ExecutionError> {
        let tx_data = Bytes::copy_from_slice(input);
        let mut inspector = CallInterceptorService {
            db: self.db.clone(),
        };

        let block_env = self.db.get_service_block_env()?;
        let result_state = {
            let mut evm: Evm<'_, _, _> = Evm::builder()
                .with_ref_db(&mut self.db)
                .with_external_context(&mut inspector)
                .modify_tx_env(|tx| {
                    tx.clear();
                    tx.transact_to = kind;
                    tx.data = tx_data;
                    tx.gas_limit = EVM_SERVICE_GAS_LIMIT;
                })
                .modify_block_env(|block| {
                    *block = block_env;
                })
                .append_handler_register(|handler| {
                    inspector_handle_register(handler);
                    let precompiles = handler.pre_execution.load_precompiles();
                    handler.pre_execution.load_precompiles = Arc::new(move || {
                        let mut precompiles = precompiles.clone();
                        precompiles.extend([(
                            PRECOMPILE_ADDRESS,
                            ContextPrecompile::ContextStateful(Arc::new(GeneralServiceCall)),
                        )]);
                        precompiles
                    });
                })
                .build();

            evm.transact().map_err(|error| {
                let error = format!("{:?}", error);
                let error = EvmExecutionError::TransactError(error);
                ExecutionError::EvmError(error)
            })
        }?;
        let storage_stats = self.db.take_storage_stats();
        Ok((
            process_execution_result(storage_stats, result_state.result)?,
            result_state.state,
        ))
    }
}
