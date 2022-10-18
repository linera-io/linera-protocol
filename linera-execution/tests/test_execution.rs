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
        _context: &OperationContext,
        storage: &dyn WritableStorageContext,
        operation: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error> {
        let mut state = storage.try_load_my_state().await?;
        state.extend(operation);
        storage.try_save_my_state(state).await?;
        Ok(RawApplicationResult::default())
    }

    /// Apply an effect originating from a cross-chain message.
    async fn apply_effect(
        &self,
        _context: &EffectContext,
        _storage: &dyn WritableStorageContext,
        _effect: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error> {
        Ok(RawApplicationResult::default())
    }

    /// Allow an operation or an effect of other applications to call into this
    /// application.
    async fn call_application(
        &self,
        _context: &CalleeContext,
        _storage: &dyn WritableStorageContext,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<RawCallResult, Error> {
        Ok(RawCallResult::default())
    }

    /// Allow an operation or an effect of other applications to call into a session that we previously created.
    async fn call_session(
        &self,
        _context: &CalleeContext,
        _storage: &dyn WritableStorageContext,
        _session_kind: u64,
        _session_data: &mut Vec<u8>,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<RawCallResult, Error> {
        Ok(RawCallResult::default())
    }

    /// Allow an end user to execute read-only queries on the state of this application.
    /// NOTE: This is not meant to be metered and may not be exposed by all validators.
    async fn query_application(
        &self,
        _context: &QueryContext,
        storage: &dyn QueryableStorageContext,
        _argument: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let state = storage.try_read_my_state().await?;
        Ok(state)
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
        .apply_operation(app_id, &context, &Operation::User(vec![1]))
        .await
        .unwrap();
    assert_eq!(
        result,
        vec![ApplicationResult::User(
            app_id,
            RawApplicationResult::default()
        )]
    );

    let context = QueryContext {
        chain_id: ChainId::root(0),
    };
    assert_eq!(
        view.query_application(app_id, &context, &Query::User(vec![]))
            .await
            .unwrap(),
        Response::User(vec![1])
    );
}
