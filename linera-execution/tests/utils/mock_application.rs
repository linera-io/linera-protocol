// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Mocking of user applications to help with execution scenario tests.

#![allow(dead_code)]

use linera_base::identifiers::SessionId;
use linera_execution::{
    ApplicationCallOutcome, CalleeContext, ContractSyncRuntime, ExecutionError, MessageContext,
    OperationContext, QueryContext, RawExecutionOutcome, ServiceSyncRuntime, SessionCallOutcome,
    UserContract, UserContractModule, UserService, UserServiceModule,
};
use std::{
    collections::VecDeque,
    fmt::{self, Display, Formatter},
    sync::{Arc, Mutex},
};

/// A mocked implementation of a user application.
///
/// Implements [`UserContractModule`], [`UserServiceModule`], [`UserContract`] and [`UserService`].
#[derive(Clone)]
pub struct MockApplication<Runtime> {
    runtime: Runtime,
    expected_calls: Arc<Mutex<VecDeque<ExpectedCall>>>,
}

impl Default for MockApplication<()> {
    fn default() -> Self {
        MockApplication {
            runtime: (),
            expected_calls: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl MockApplication<()> {
    /// Configures the `runtime` to use with the [`MockApplication`] instance.
    pub fn with_runtime<NewRuntime>(self, runtime: NewRuntime) -> MockApplication<NewRuntime> {
        MockApplication {
            runtime,
            expected_calls: self.expected_calls,
        }
    }
}

impl<Runtime> MockApplication<Runtime> {
    /// Queues an expected call to the [`MockApplication`].
    pub fn expect_call(&self, expected_call: ExpectedCall) {
        self.expected_calls
            .lock()
            .expect("Mutex is poisoned")
            .push_back(expected_call);
    }
}

impl<Runtime> MockApplication<Runtime> {
    /// Retrieves the next [`ExpectedCall`] in the queue.
    fn next_expected_call(&mut self) -> Option<ExpectedCall> {
        self.expected_calls
            .lock()
            .expect("Mutex is poisoned")
            .pop_front()
    }
}

type InitializeHandler = Box<
    dyn FnOnce(
            &mut ContractSyncRuntime,
            OperationContext,
            Vec<u8>,
        ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError>
        + Send
        + Sync,
>;
type ExecuteOperationHandler = Box<
    dyn FnOnce(
            &mut ContractSyncRuntime,
            OperationContext,
            Vec<u8>,
        ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError>
        + Send
        + Sync,
>;
type ExecuteMessageHandler = Box<
    dyn FnOnce(
            &mut ContractSyncRuntime,
            MessageContext,
            Vec<u8>,
        ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError>
        + Send
        + Sync,
>;
type HandleApplicationCallHandler = Box<
    dyn FnOnce(
            &mut ContractSyncRuntime,
            CalleeContext,
            Vec<u8>,
            Vec<SessionId>,
        ) -> Result<ApplicationCallOutcome, ExecutionError>
        + Send
        + Sync,
>;
type HandleSessionCallHandler = Box<
    dyn FnOnce(
            &mut ContractSyncRuntime,
            CalleeContext,
            Vec<u8>,
            Vec<u8>,
            Vec<SessionId>,
        ) -> Result<(SessionCallOutcome, Vec<u8>), ExecutionError>
        + Send
        + Sync,
>;
type HandleQueryHandler = Box<
    dyn FnOnce(&mut ServiceSyncRuntime, QueryContext, Vec<u8>) -> Result<Vec<u8>, ExecutionError>
        + Send
        + Sync,
>;

/// An expected call to a [`MockApplication`].
pub enum ExpectedCall {
    /// An expected call to [`UserContract::initialize`].
    Initialize(InitializeHandler),
    /// An expected call to [`UserContract::execute_operation`].
    ExecuteOperation(ExecuteOperationHandler),
    /// An expected call to [`UserContract::execute_message`].
    ExecuteMessage(ExecuteMessageHandler),
    /// An expected call to [`UserContract::handle_application_call`].
    HandleApplicationCall(HandleApplicationCallHandler),
    /// An expected call to [`UserContract::handle_session_call`].
    HandleSessionCall(HandleSessionCallHandler),
    /// An expected call to [`UserService::handle_query`].
    HandleQuery(HandleQueryHandler),
}

impl Display for ExpectedCall {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        let name = match self {
            ExpectedCall::Initialize(_) => "initialize",
            ExpectedCall::ExecuteOperation(_) => "execute_operation",
            ExpectedCall::ExecuteMessage(_) => "execute_message",
            ExpectedCall::HandleApplicationCall(_) => "handle_application_call",
            ExpectedCall::HandleSessionCall(_) => "handle_session_call",
            ExpectedCall::HandleQuery(_) => "handle_query",
        };

        write!(formatter, "{name}")
    }
}

impl ExpectedCall {
    /// Creates an [`ExpectedCall`] to the [`MockApplication`]'s [`UserContract::initialize`]
    /// implementation, which is handled by the provided `handler`.
    pub fn initialize(
        handler: impl FnOnce(
                &mut ContractSyncRuntime,
                OperationContext,
                Vec<u8>,
            ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        ExpectedCall::Initialize(Box::new(handler))
    }

    /// Creates an [`ExpectedCall`] to the [`MockApplication`]'s
    /// [`UserContract::execute_operation`] implementation, which is handled by the provided
    /// `handler`.
    pub fn execute_operation(
        handler: impl FnOnce(
                &mut ContractSyncRuntime,
                OperationContext,
                Vec<u8>,
            ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        ExpectedCall::ExecuteOperation(Box::new(handler))
    }

    /// Creates an [`ExpectedCall`] to the [`MockApplication`]'s
    /// [`UserContract::execute_message`] implementation, which is handled by the provided
    /// `handler`.
    pub fn execute_message(
        handler: impl FnOnce(
                &mut ContractSyncRuntime,
                MessageContext,
                Vec<u8>,
            ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        ExpectedCall::ExecuteMessage(Box::new(handler))
    }

    /// Creates an [`ExpectedCall`] to the [`MockApplication`]'s
    /// [`UserContract::handle_application_call`] implementation, which is handled by the provided
    /// `handler`.
    pub fn handle_application_call(
        handler: impl FnOnce(
                &mut ContractSyncRuntime,
                CalleeContext,
                Vec<u8>,
                Vec<SessionId>,
            ) -> Result<ApplicationCallOutcome, ExecutionError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        ExpectedCall::HandleApplicationCall(Box::new(handler))
    }

    /// Creates an [`ExpectedCall`] to the [`MockApplication`]'s
    /// [`UserContract::handle_session_call`] implementation, which is handled by the provided
    /// `handler`.
    pub fn handle_session_call(
        handler: impl FnOnce(
                &mut ContractSyncRuntime,
                CalleeContext,
                Vec<u8>,
                Vec<u8>,
                Vec<SessionId>,
            ) -> Result<(SessionCallOutcome, Vec<u8>), ExecutionError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        ExpectedCall::HandleSessionCall(Box::new(handler))
    }

    /// Creates an [`ExpectedCall`] to the [`MockApplication`]'s [`UserService::handle_query`]
    /// implementation, which is handled by the provided `handler`.
    pub fn handle_query(
        handler: impl FnOnce(
                &mut ServiceSyncRuntime,
                QueryContext,
                Vec<u8>,
            ) -> Result<Vec<u8>, ExecutionError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        ExpectedCall::HandleQuery(Box::new(handler))
    }
}

impl UserContractModule for MockApplication<()> {
    fn instantiate(
        &self,
        runtime: ContractSyncRuntime,
    ) -> Result<Box<dyn UserContract + Send + Sync + 'static>, ExecutionError> {
        Ok(Box::new(self.clone().with_runtime(runtime)))
    }
}

impl UserServiceModule for MockApplication<()> {
    fn instantiate(
        &self,
        runtime: ServiceSyncRuntime,
    ) -> Result<Box<dyn UserService + Send + Sync + 'static>, ExecutionError> {
        Ok(Box::new(self.clone().with_runtime(runtime)))
    }
}

impl UserContract for MockApplication<ContractSyncRuntime> {
    fn initialize(
        &mut self,
        context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError> {
        match self.next_expected_call() {
            Some(ExpectedCall::Initialize(handler)) => {
                handler(&mut self.runtime, context, argument)
            }
            Some(unexpected_call) => panic!(
                "Expected a call to `initialize`, got a call to `{unexpected_call}` instead."
            ),
            None => panic!("Unexpected call to `initialize`"),
        }
    }

    fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError> {
        match self.next_expected_call() {
            Some(ExpectedCall::ExecuteOperation(handler)) => {
                handler(&mut self.runtime, context, operation)
            }
            Some(unexpected_call) => panic!(
                "Expected a call to `execute_operation`, got a call to `{unexpected_call}` instead."
            ),
            None => panic!("Unexpected call to `execute_operation`"),
        }
    }

    fn execute_message(
        &mut self,
        context: MessageContext,
        message: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError> {
        match self.next_expected_call() {
            Some(ExpectedCall::ExecuteMessage(handler)) => {
                handler(&mut self.runtime, context, message)
            }
            Some(unexpected_call) => panic!(
                "Expected a call to `execute_message`, got a call to `{unexpected_call}` instead."
            ),
            None => panic!("Unexpected call to `execute_message`"),
        }
    }

    fn handle_application_call(
        &mut self,
        context: CalleeContext,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallOutcome, ExecutionError> {
        match self.next_expected_call() {
            Some(ExpectedCall::HandleApplicationCall(handler)) => {
                handler(&mut self.runtime, context, argument, forwarded_sessions)
            }
            Some(unexpected_call) => panic!(
                "Expected a call to `handle_application_call`, got a call to `{unexpected_call}` instead."
            ),
            None => panic!("Unexpected call to `handle_application_call`"),
        }
    }

    fn handle_session_call(
        &mut self,
        context: CalleeContext,
        session_state: Vec<u8>,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<(SessionCallOutcome, Vec<u8>), ExecutionError> {
        match self.next_expected_call() {
            Some(ExpectedCall::HandleSessionCall(handler)) => {
                handler(&mut self.runtime, context, session_state, argument, forwarded_sessions)
            }
            Some(unexpected_call) => panic!(
                "Expected a call to `handle_session_call`, got a call to `{unexpected_call}` instead."
            ),
            None => panic!("Unexpected call to `handle_session_call`"),
        }
    }
}

impl UserService for MockApplication<ServiceSyncRuntime> {
    fn handle_query(
        &mut self,
        context: QueryContext,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        match self.next_expected_call() {
            Some(ExpectedCall::HandleQuery(handler)) => handler(&mut self.runtime, context, query),
            Some(unexpected_call) => panic!(
                "Expected a call to `handle_query`, got a call to `{unexpected_call}` instead."
            ),
            None => panic!("Unexpected call to `handle_query`"),
        }
    }
}
