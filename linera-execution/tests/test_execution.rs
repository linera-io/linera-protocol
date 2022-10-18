// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use async_trait::async_trait;
use linera_base::{
    error::Error,
    messages::{ApplicationId, BlockHeight, ChainDescription, ChainId},
};
use linera_execution::*;
use linera_views::{
    memory::MemoryContext,
    views::{Context, View},
};
use std::sync::Arc;

#[tokio::test]
async fn test_missing_user_application() {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;

    let app_id = ApplicationId(1);

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
    };
    assert_eq!(
        view.apply_operation(app_id, &context, &Operation::User(vec![]))
            .await,
        Err(Error::UnknownApplication)
    );
}

struct TestApplication;

#[async_trait]
impl UserApplication for TestApplication {
    /// Apply an operation from the current block.
    async fn apply_operation(
        &self,
        context: &OperationContext,
        storage: &dyn WritableStorageContext,
        operation: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error> {
        Ok(RawApplicationResult::default())
    }

    /// Apply an effect originating from a cross-chain message.
    async fn apply_effect(
        &self,
        context: &EffectContext,
        storage: &dyn WritableStorageContext,
        effect: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error> {
        Ok(RawApplicationResult::default())
    }

    /// Allow an operation or an effect of other applications to call into this
    /// application.
    async fn call_application(
        &self,
        context: &CalleeContext,
        storage: &dyn WritableStorageContext,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<RawCallResult, Error> {
        Ok(RawCallResult::default())
    }

    /// Allow an operation or an effect of other applications to call into a session that we previously created.
    async fn call_session(
        &self,
        context: &CalleeContext,
        storage: &dyn WritableStorageContext,
        session_kind: u64,
        session_data: &mut Vec<u8>,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<RawCallResult, Error> {
        Ok(RawCallResult::default())
    }

    /// Allow an end user to execute read-only queries on the state of this application.
    /// NOTE: This is not meant to be metered and may not be exposed by all validators.
    async fn query_application(
        &self,
        context: &QueryContext,
        storage: &dyn QueryableStorageContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn test_simple_user_operation() {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let app_id = ApplicationId(1);
    view.context()
        .extra()
        .user_applications()
        .insert(app_id, Arc::new(TestApplication));

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
    };
    let result = view
        .apply_operation(app_id, &context, &Operation::User(vec![]))
        .await
        .unwrap();
    assert_eq!(
        result,
        vec![ApplicationResult::User(
            app_id,
            RawApplicationResult::default()
        )]
    );
}
