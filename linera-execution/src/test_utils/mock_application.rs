// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Mocking of user applications to help with execution scenario tests.

#![allow(dead_code)]

use std::{
    collections::VecDeque,
    fmt::{self, Display, Formatter},
    mem,
    sync::{Arc, Mutex},
};

use crate::{
    ContractSyncRuntimeHandle, ExecutionError, FinalizeContext, MessageContext, OperationContext,
    QueryContext, ServiceSyncRuntimeHandle, UserContract, UserContractModule, UserService,
    UserServiceModule,
};

/// A mocked implementation of a user application.
///
/// Should be configured with any expected calls, and can then be used to create a
/// [`MockApplicationInstance`] that implements [`UserContract`] and [`UserService`].
#[derive(Clone, Default)]
pub struct MockApplication {
    expected_calls: Arc<Mutex<VecDeque<ExpectedCall>>>,
}

/// A mocked implementation of a user application instance.
///
/// Will expect certain calls previously configured through [`MockApplication`].
pub struct MockApplicationInstance<Runtime> {
    expected_calls: VecDeque<ExpectedCall>,
    runtime: Runtime,
}

impl MockApplication {
    /// Queues an expected call to the [`MockApplication`].
    pub fn expect_call(&self, expected_call: ExpectedCall) {
        self.expected_calls
            .lock()
            .expect("Mutex is poisoned")
            .push_back(expected_call);
    }

    /// Creates a new [`MockApplicationInstance`], forwarding the configured expected calls.
    pub fn create_mock_instance<Runtime>(
        &self,
        runtime: Runtime,
    ) -> MockApplicationInstance<Runtime> {
        MockApplicationInstance {
            expected_calls: mem::take(&mut self.expected_calls.lock().expect("Mutex is poisoned")),
            runtime,
        }
    }
}

type InstantiateHandler = Box<
    dyn FnOnce(
            &mut ContractSyncRuntimeHandle,
            OperationContext,
            Vec<u8>,
        ) -> Result<(), ExecutionError>
        + Send
        + Sync,
>;
type ExecuteOperationHandler = Box<
    dyn FnOnce(
            &mut ContractSyncRuntimeHandle,
            OperationContext,
            Vec<u8>,
        ) -> Result<Vec<u8>, ExecutionError>
        + Send
        + Sync,
>;
type ExecuteMessageHandler = Box<
    dyn FnOnce(
            &mut ContractSyncRuntimeHandle,
            MessageContext,
            Vec<u8>,
        ) -> Result<(), ExecutionError>
        + Send
        + Sync,
>;
type FinalizeHandler = Box<
    dyn FnOnce(&mut ContractSyncRuntimeHandle, FinalizeContext) -> Result<(), ExecutionError>
        + Send
        + Sync,
>;
type HandleQueryHandler = Box<
    dyn FnOnce(
            &mut ServiceSyncRuntimeHandle,
            QueryContext,
            Vec<u8>,
        ) -> Result<Vec<u8>, ExecutionError>
        + Send
        + Sync,
>;

/// An expected call to a [`MockApplicationInstance`].
pub enum ExpectedCall {
    /// An expected call to [`UserContract::instantiate`].
    Instantiate(InstantiateHandler),
    /// An expected call to [`UserContract::execute_operation`].
    ExecuteOperation(ExecuteOperationHandler),
    /// An expected call to [`UserContract::execute_message`].
    ExecuteMessage(ExecuteMessageHandler),
    /// An expected call to [`UserContract::finalize`].
    Finalize(FinalizeHandler),
    /// An expected call to [`UserService::handle_query`].
    HandleQuery(HandleQueryHandler),
}

impl Display for ExpectedCall {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        let name = match self {
            ExpectedCall::Instantiate(_) => "instantiate",
            ExpectedCall::ExecuteOperation(_) => "execute_operation",
            ExpectedCall::ExecuteMessage(_) => "execute_message",
            ExpectedCall::Finalize(_) => "finalize",
            ExpectedCall::HandleQuery(_) => "handle_query",
        };

        write!(formatter, "{name}")
    }
}

impl ExpectedCall {
    /// Creates an [`ExpectedCall`] to the [`MockApplicationInstance`]'s
    /// [`UserContract::instantiate`] implementation, which is handled by the provided `handler`.
    pub fn instantiate(
        handler: impl FnOnce(
                &mut ContractSyncRuntimeHandle,
                OperationContext,
                Vec<u8>,
            ) -> Result<(), ExecutionError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        ExpectedCall::Instantiate(Box::new(handler))
    }

    /// Creates an [`ExpectedCall`] to the [`MockApplicationInstance`]'s
    /// [`UserContract::execute_operation`] implementation, which is handled by the provided
    /// `handler`.
    pub fn execute_operation(
        handler: impl FnOnce(
                &mut ContractSyncRuntimeHandle,
                OperationContext,
                Vec<u8>,
            ) -> Result<Vec<u8>, ExecutionError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        ExpectedCall::ExecuteOperation(Box::new(handler))
    }

    /// Creates an [`ExpectedCall`] to the [`MockApplicationInstance`]'s
    /// [`UserContract::execute_message`] implementation, which is handled by the provided
    /// `handler`.
    pub fn execute_message(
        handler: impl FnOnce(
                &mut ContractSyncRuntimeHandle,
                MessageContext,
                Vec<u8>,
            ) -> Result<(), ExecutionError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        ExpectedCall::ExecuteMessage(Box::new(handler))
    }

    /// Creates an [`ExpectedCall`] to the [`MockApplicationInstance`]'s [`UserContract::finalize`]
    /// implementation, which is handled by the provided `handler`.
    pub fn finalize(
        handler: impl FnOnce(&mut ContractSyncRuntimeHandle, FinalizeContext) -> Result<(), ExecutionError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        ExpectedCall::Finalize(Box::new(handler))
    }

    /// Creates an [`ExpectedCall`] to the [`MockApplicationInstance`]'s [`UserContract::finalize`]
    /// implementation, which is handled by the default implementation which does nothing.
    pub fn default_finalize() -> Self {
        Self::finalize(|_, _| Ok(()))
    }

    /// Creates an [`ExpectedCall`] to the [`MockApplicationInstance`]'s
    /// [`UserService::handle_query`] implementation, which is handled by the provided `handler`.
    pub fn handle_query(
        handler: impl FnOnce(
                &mut ServiceSyncRuntimeHandle,
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

impl UserContractModule for MockApplication {
    fn instantiate(
        &self,
        runtime: ContractSyncRuntimeHandle,
    ) -> Result<Box<dyn UserContract + 'static>, ExecutionError> {
        Ok(Box::new(self.create_mock_instance(runtime)))
    }
}

impl UserServiceModule for MockApplication {
    fn instantiate(
        &self,
        runtime: ServiceSyncRuntimeHandle,
    ) -> Result<Box<dyn UserService + 'static>, ExecutionError> {
        Ok(Box::new(self.create_mock_instance(runtime)))
    }
}

impl<Runtime> MockApplicationInstance<Runtime> {
    /// Retrieves the next [`ExpectedCall`] in the queue.
    fn next_expected_call(&mut self) -> Option<ExpectedCall> {
        self.expected_calls.pop_front()
    }
}

impl UserContract for MockApplicationInstance<ContractSyncRuntimeHandle> {
    fn instantiate(
        &mut self,
        context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        match self.next_expected_call() {
            Some(ExpectedCall::Instantiate(handler)) => {
                handler(&mut self.runtime, context, argument)
            }
            Some(unexpected_call) => panic!(
                "Expected a call to `instantiate`, got a call to `{unexpected_call}` instead."
            ),
            None => panic!("Unexpected call to `instantiate`"),
        }
    }

    fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
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
    ) -> Result<(), ExecutionError> {
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

    fn finalize(&mut self, context: FinalizeContext) -> Result<(), ExecutionError> {
        match self.next_expected_call() {
            Some(ExpectedCall::Finalize(handler)) => handler(&mut self.runtime, context),
            Some(unexpected_call) => {
                panic!("Expected a call to `finalize`, got a call to `{unexpected_call}` instead.")
            }
            None => panic!("Unexpected call to `finalize`"),
        }
    }
}

impl UserService for MockApplicationInstance<ServiceSyncRuntimeHandle> {
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
