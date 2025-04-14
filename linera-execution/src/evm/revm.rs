// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Revm](https://bluealloy.github.io/revm/) runtime.

use core::ops::Range;
use std::sync::Arc;

use alloy::primitives::Address;
use linera_base::{
    data_types::Bytecode,
    ensure,
    identifiers::{ApplicationId, StreamName},
    vm::EvmQuery,
};
use revm::{
    db::WrapDatabaseRef, inspector_handle_register, primitives::Bytes, ContextPrecompile,
    ContextStatefulPrecompile, Evm, EvmContext, InnerEvmContext, Inspector,
};
use revm_interpreter::{CallInputs, CallOutcome, Gas, InstructionResult, InterpreterResult};
use revm_precompile::PrecompileResult;
use revm_primitives::{
    address, ExecutionResult, Log, Output, PrecompileErrors, PrecompileOutput, SuccessReason,
    TxKind,
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
    evm::database::DatabaseRuntime, ContractRuntime, ContractSyncRuntimeHandle, EvmExecutionError,
    EvmRuntime, ExecutionError, FinalizeContext, MessageContext, OperationContext, QueryContext,
    ServiceRuntime, ServiceSyncRuntimeHandle, UserContract, UserContractInstance,
    UserContractModule, UserService, UserServiceInstance, UserServiceModule,
};

/// This is the selector of the `execute_message` that should be called
/// only from a submitted message
const EXECUTE_MESSAGE_SELECTOR: &[u8] = &[173, 125, 234, 205];

fn forbid_execute_operation_origin(vec: &[u8]) -> Result<(), ExecutionError> {
    ensure!(
        vec != EXECUTE_MESSAGE_SELECTOR,
        ExecutionError::EvmError(EvmExecutionError::OperationCallExecuteMessage)
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

    use crate::evm::revm::EXECUTE_MESSAGE_SELECTOR;

    // The function keccak256 is not const so we cannot build the execute_message
    // selector directly.
    #[test]
    fn check_execute_message_selector() {
        let selector = &keccak256("execute_message(bytes)".as_bytes())[..4];
        assert_eq!(selector, EXECUTE_MESSAGE_SELECTOR);
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
    let mut output = [0u64; 4];
    for (i, chunk) in vec.chunks_exact(8).enumerate() {
        output[i] = u64::from_be_bytes(chunk.try_into().unwrap());
    }
    let hash = output.into();
    ApplicationId::new(hash)
}

fn address_to_user_application_id(address: Address) -> ApplicationId {
    let mut vec = vec![0_u8; 32];
    vec[..20].copy_from_slice(address.as_ref());
    u8_slice_to_application_id(&vec)
}

struct GeneralContractCall;

impl<Runtime: ContractRuntime>
    ContextStatefulPrecompile<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>
    for GeneralContractCall
{
    fn call(
        &self,
        input: &Bytes,
        _gas_limit: u64,
        context: &mut InnerEvmContext<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
    ) -> PrecompileResult {
        let vec = input.to_vec();
        let target = u8_slice_to_application_id(&vec[0..32]);
        let argument: Vec<u8> = vec[32..].to_vec();
        let result = {
            let authenticated = true;
            let mut runtime = context
                .db
                .0
                .runtime
                .lock()
                .expect("The lock should be possible");
            runtime.try_call_application(authenticated, target, argument)
        }
        .map_err(|error| PrecompileErrors::Fatal {
            msg: format!("{}", error),
        })?;
        // We do not know how much gas was used.
        let gas_used = 0;
        let bytes = Bytes::copy_from_slice(&result);
        let result = PrecompileOutput { gas_used, bytes };
        Ok(result)
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
        _gas_limit: u64,
        context: &mut InnerEvmContext<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
    ) -> PrecompileResult {
        let vec = input.to_vec();
        let target = u8_slice_to_application_id(&vec[0..32]);
        let argument: Vec<u8> = vec[32..].to_vec();
        let result = {
            let mut runtime = context
                .db
                .0
                .runtime
                .lock()
                .expect("The lock should be possible");
            runtime.try_query_application(target, argument)
        }
        .map_err(|error| PrecompileErrors::Fatal {
            msg: format!("{}", error),
        })?;
        // We do not know how much gas was used.
        let gas_used = 0;
        let bytes = Bytes::copy_from_slice(&result);
        let result = PrecompileOutput { gas_used, bytes };
        Ok(result)
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
        let result = bcs::from_bytes::<InterpreterResult>(&result)?;
        let call_outcome = CallOutcome {
            result,
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
        let result = bcs::from_bytes::<InterpreterResult>(&result)?;
        let call_outcome = CallOutcome {
            result,
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

struct ExecutionResultSuccess {
    reason: SuccessReason,
    logs: Vec<Log>,
    output: Output,
}

impl ExecutionResultSuccess {
    fn interpreter_result_and_logs(self) -> Result<(Vec<u8>, Vec<Log>), ExecutionError> {
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
        Ok((result, self.logs))
    }

    fn output_and_logs(self) -> (Vec<u8>, Vec<Log>) {
        let Output::Call(output) = self.output else {
            unreachable!("It is impossible for a Choice::Call to lead to an Output::Create");
        };
        let output = output.as_ref().to_vec();
        (output, self.logs)
    }
}

impl<Runtime> UserContract for RevmContractInstance<Runtime>
where
    Runtime: ContractRuntime,
{
    fn instantiate(
        &mut self,
        _context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        let argument = serde_json::from_slice::<Vec<u8>>(&argument)?;
        let mut vec = self.module.clone();
        vec.extend_from_slice(&argument);
        let result = self.transact_commit_tx_data(Choice::Create, &vec)?;
        self.write_logs(result.logs, "deploy")?;
        let Output::Create(_, _) = result.output else {
            unreachable!("It is impossible for a Choice::Create to lead to an Output::Call");
        };
        Ok(())
    }

    fn execute_operation(
        &mut self,
        _context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        ensure_message_length(operation.len(), 4)?;
        let (output, logs) = if &operation[..4] == INTERPRETER_RESULT_SELECTOR {
            let result = self.transact_commit_tx_data(Choice::Call, &operation[4..])?;
            result.interpreter_result_and_logs()?
        } else {
            let result = self.transact_commit_tx_data(Choice::Call, &operation)?;
            result.output_and_logs()
        };
        self.write_logs(logs, "operation")?;
        Ok(output)
    }

    fn execute_message(
        &mut self,
        _context: MessageContext,
        _message: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        // TODO(#3760): Implement execute_message for EVM
        todo!("The execute_message part of the Ethereum smart contract has not yet been coded");
    }

    fn finalize(&mut self, _context: FinalizeContext) -> Result<(), ExecutionError> {
        Ok(())
    }
}

fn process_execution_result(
    result: ExecutionResult,
) -> Result<ExecutionResultSuccess, ExecutionError> {
    match result {
        ExecutionResult::Success {
            reason,
            gas_used: _,
            gas_refunded: _,
            logs,
            output,
        } => Ok(ExecutionResultSuccess {
            reason,
            logs,
            output,
        }),
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

    fn transact_commit_tx_data(
        &mut self,
        ch: Choice,
        vec: &[u8],
    ) -> Result<ExecutionResultSuccess, ExecutionError> {
        let (kind, tx_data) = match ch {
            Choice::Create => (TxKind::Create, Bytes::copy_from_slice(vec)),
            Choice::Call => {
                ensure_message_length(vec.len(), 4)?;
                forbid_execute_operation_origin(&vec[..4])?;
                let tx_data = Bytes::copy_from_slice(vec);
                (TxKind::Call(Address::ZERO.create(0)), tx_data)
            }
        };
        let mut inspector = CallInterceptorContract {
            db: self.db.clone(),
        };
        let mut evm: Evm<'_, _, _> = Evm::builder()
            .with_ref_db(&mut self.db)
            .with_external_context(&mut inspector)
            .modify_tx_env(|tx| {
                tx.clear();
                tx.transact_to = kind;
                tx.data = tx_data;
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

        let result = evm.transact_commit().map_err(|error| {
            let error = format!("{:?}", error);
            let error = EvmExecutionError::TransactCommitError(error);
            ExecutionError::EvmError(error)
        })?;
        process_execution_result(result)
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
    db: DatabaseRuntime<Runtime>,
}

impl<Runtime> RevmServiceInstance<Runtime>
where
    Runtime: ServiceRuntime,
{
    pub fn prepare(_module: Vec<u8>, runtime: Runtime) -> Self {
        let db = DatabaseRuntime::new(runtime);
        Self { db }
    }
}

impl<Runtime> UserService for RevmServiceInstance<Runtime>
where
    Runtime: ServiceRuntime,
{
    fn handle_query(
        &mut self,
        _context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
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
        let answer = if &query[..4] == INTERPRETER_RESULT_SELECTOR {
            let result = self.transact_tx_data(&query[4..])?;
            let (answer, _logs) = result.interpreter_result_and_logs()?;
            answer
        } else {
            let result = self.transact_tx_data(&query)?;
            let (output, _logs) = result.output_and_logs();
            serde_json::to_vec(&output)?
        };
        // We drop the logs since the "eth_call" execution does not return any log.
        Ok(answer)
    }
}

impl<Runtime> RevmServiceInstance<Runtime>
where
    Runtime: ServiceRuntime,
{
    fn transact_tx_data(&mut self, vec: &[u8]) -> Result<ExecutionResultSuccess, ExecutionError> {
        ensure_message_length(vec.len(), 4)?;
        forbid_execute_operation_origin(&vec[..4])?;
        let tx_data = Bytes::copy_from_slice(vec);
        let contract_address = Address::ZERO.create(0);
        let mut inspector = CallInterceptorService {
            db: self.db.clone(),
        };

        let mut evm: Evm<'_, _, _> = Evm::builder()
            .with_ref_db(&mut self.db)
            .with_external_context(&mut inspector)
            .modify_tx_env(|tx| {
                tx.clear();
                tx.transact_to = TxKind::Call(contract_address);
                tx.data = tx_data;
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

        let result_state = evm.transact().map_err(|error| {
            let error = format!("{:?}", error);
            let error = EvmExecutionError::TransactCommitError(error);
            ExecutionError::EvmError(error)
        })?;
        process_execution_result(result_state.result)
    }
}
