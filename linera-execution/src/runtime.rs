// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    execution::{ExecutionStateView, ExecutionStateViewContext},
    ApplicationResult, CallResult, ExecutionRuntimeContext, NewSession, QueryableStorageContext,
    ReadableStorageContext, SessionId, WritableStorageContext,
};
use async_trait::async_trait;
use linera_base::{
    ensure,
    error::Error,
    messages::{ApplicationId, ChainId},
};
use linera_views::views::{RegisterView, View};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::{Mutex, MutexGuard, OwnedMutexGuard};

#[derive(Debug, Clone)]
pub(crate) struct ExecutionRuntime<'a, C, const WRITABLE: bool> {
    chain_id: ChainId,
    application_id: ApplicationId,
    execution_state: Arc<Mutex<&'a mut ExecutionStateView<C>>>,
    session_manager: Arc<Mutex<&'a mut SessionManager>>,
    active_user_states: Arc<Mutex<ActiveUserStates<C>>>,
    active_sessions: Arc<Mutex<ActiveSessions>>,
    application_results: Arc<Mutex<&'a mut Vec<ApplicationResult>>>,
}

type ActiveUserStates<C> = BTreeMap<ApplicationId, OwnedMutexGuard<RegisterView<C, Vec<u8>>>>;

type ActiveSessions = BTreeMap<SessionId, OwnedMutexGuard<SessionState>>;

#[derive(Debug, Clone, Default)]
pub(crate) struct SessionManager {
    pub(crate) counters: BTreeMap<ApplicationId, u64>,
    // We prevent all re-entrant calls and changes of ownership when a session is being called.
    pub(crate) states: BTreeMap<SessionId, Arc<Mutex<SessionState>>>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SessionState {
    owner: ApplicationId,
    data: Vec<u8>,
}

impl<'a, C, const W: bool> ExecutionRuntime<'a, C, W> {
    pub fn new(
        chain_id: ChainId,
        application_id: ApplicationId,
        execution_state: &'a mut ExecutionStateView<C>,
        session_manager: &'a mut SessionManager,
        application_results: &'a mut Vec<ApplicationResult>,
    ) -> Self {
        Self {
            chain_id,
            application_id,
            execution_state: Arc::new(Mutex::new(execution_state)),
            session_manager: Arc::new(Mutex::new(session_manager)),
            active_user_states: Arc::default(),
            active_sessions: Arc::default(),
            application_results: Arc::new(Mutex::new(application_results)),
        }
    }
}

impl<'a, C, const W: bool> ExecutionRuntime<'a, C, W>
where
    C: ExecutionStateViewContext,
    C::Extra: ExecutionRuntimeContext,
    Error: From<C::Error>,
{
    fn try_lock_execution_state(
        &self,
    ) -> Result<MutexGuard<'_, &'a mut ExecutionStateView<C>>, C::Error> {
        Ok(self.execution_state.try_lock()?)
    }

    fn try_lock_session_manager(&self) -> Result<MutexGuard<'_, &'a mut SessionManager>, C::Error> {
        Ok(self.session_manager.try_lock()?)
    }

    fn try_lock_active_user_states(&self) -> Result<MutexGuard<'_, ActiveUserStates<C>>, C::Error> {
        Ok(self.active_user_states.try_lock()?)
    }

    fn try_lock_active_sessions(&self) -> Result<MutexGuard<'_, ActiveSessions>, C::Error> {
        Ok(self.active_sessions.try_lock()?)
    }

    fn try_lock_application_results(
        &self,
    ) -> Result<MutexGuard<'_, &'a mut Vec<ApplicationResult>>, C::Error> {
        Ok(self.application_results.try_lock()?)
    }

    fn forward_sessions(
        &self,
        session_ids: &[SessionId],
        from_id: ApplicationId,
        to_id: ApplicationId,
    ) -> Result<(), Error> {
        let states = &self.try_lock_session_manager()?.states;
        for id in session_ids {
            let mut state = states
                .get(id)
                .ok_or(Error::InvalidSession)?
                .clone()
                .try_lock_owned()
                .map_err(C::Error::from)?;
            // Verify ownership.
            ensure!(state.owner == from_id, Error::InvalidSessionOwner);
            // Transfer the session.
            state.owner = to_id;
        }
        Ok(())
    }

    fn make_sessions(
        &self,
        new_sessions: Vec<NewSession>,
        creator_id: ApplicationId,
        receiver_id: ApplicationId,
    ) -> Result<Vec<SessionId>, Error> {
        let mut manager = self.try_lock_session_manager()?;
        let mut counter = manager
            .counters
            .get(&creator_id)
            .copied()
            .unwrap_or_default();
        let states = &mut manager.states;
        let mut session_ids = Vec::new();
        for session in new_sessions {
            let id = SessionId {
                application_id: creator_id,
                kind: session.kind,
                index: counter,
            };
            session_ids.push(id);
            counter += 1;
            let state = SessionState {
                owner: receiver_id,
                data: session.data,
            };
            states.insert(id, Arc::new(Mutex::new(state)));
        }
        manager.counters.insert(creator_id, counter);
        Ok(session_ids)
    }

    fn try_load_session(
        &self,
        session_id: SessionId,
        application_id: ApplicationId,
    ) -> Result<Vec<u8>, Error> {
        let guard = self
            .try_lock_session_manager()
            .map_err(C::Error::from)?
            .states
            .get(&session_id)
            .ok_or(Error::InvalidSession)?
            .clone()
            .try_lock_owned()
            .map_err(C::Error::from)?;
        let state = guard.data.clone();
        // Verify ownership.
        ensure!(guard.owner == application_id, Error::InvalidSessionOwner);
        // Remember the guard. This will prevent reentrancy.
        self.try_lock_active_sessions()?.insert(session_id, guard);
        Ok(state)
    }

    fn try_save_session(
        &self,
        session_id: SessionId,
        application_id: ApplicationId,
        state: Vec<u8>,
    ) -> Result<(), Error> {
        // Remove the guard.
        if let Some(mut guard) = self.try_lock_active_sessions()?.remove(&session_id) {
            // Verify ownership.
            ensure!(guard.owner == application_id, Error::InvalidSessionOwner);
            // Save the state and unlock the session for future calls.
            guard.data = state;
        }
        Ok(())
    }

    fn try_close_session(
        &self,
        session_id: SessionId,
        application_id: ApplicationId,
    ) -> Result<(), Error> {
        // Remove the guard.
        if let Some(guard) = self.try_lock_active_sessions()?.remove(&session_id) {
            // Verify ownership.
            ensure!(guard.owner == application_id, Error::InvalidSessionOwner);
            // Delete the session.
            self.try_lock_active_sessions()?.remove(&session_id);
        }
        Ok(())
    }
}

#[async_trait]
impl<'a, C, const W: bool> ReadableStorageContext for ExecutionRuntime<'a, C, W>
where
    C: ExecutionStateViewContext,
    C::Extra: ExecutionRuntimeContext,
    Error: From<C::Error>,
{
    async fn try_read_system_balance(&self) -> Result<crate::system::Balance, Error> {
        let value = *self.try_lock_execution_state()?.system.balance.get();
        Ok(value)
    }

    async fn try_read_my_state(&self) -> Result<Vec<u8>, Error> {
        let state = self
            .try_lock_execution_state()?
            .users
            .try_load_entry(self.application_id)
            .await?
            .get()
            .to_vec();
        Ok(state)
    }
}

#[async_trait]
impl<'a, C> QueryableStorageContext for ExecutionRuntime<'a, C, false>
where
    C: ExecutionStateViewContext,
    C::Extra: ExecutionRuntimeContext,
    Error: From<C::Error>,
{
    /// Note that queries are not available from writable contexts.
    async fn try_query_application(
        &self,
        callee_id: ApplicationId,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let application = self
            .try_lock_execution_state()?
            .context()
            .extra()
            .get_user_application(callee_id)?;
        let query_context = crate::QueryContext {
            chain_id: self.chain_id,
        };
        let value = application
            .query_application(&query_context, self, argument)
            .await?;
        Ok(value)
    }
}

#[async_trait]
impl<'a, C> WritableStorageContext for ExecutionRuntime<'a, C, true>
where
    C: ExecutionStateViewContext,
    C::Extra: ExecutionRuntimeContext,
    Error: From<C::Error>,
{
    async fn try_load_my_state(&self) -> Result<Vec<u8>, Error> {
        let view = self
            .try_lock_execution_state()?
            .users
            .try_load_entry(self.application_id)
            .await?;
        let state = view.get().to_vec();
        // Remember the view. This will prevent reentrancy.
        self.try_lock_active_user_states()?
            .insert(self.application_id, view);
        Ok(state)
    }

    async fn try_save_my_state(&self, state: Vec<u8>) -> Result<(), Error> {
        if let Some(mut view) = self
            .try_lock_active_user_states()?
            .remove(&self.application_id)
        {
            // Make the view available again.
            view.set(state);
        }
        Ok(())
    }

    async fn try_call_application(
        &self,
        authenticated: bool,
        callee_id: ApplicationId,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, Error> {
        // Load the application.
        let application = self
            .try_lock_execution_state()?
            .context()
            .extra()
            .get_user_application(callee_id)?;
        // Change the owners of forwarded sessions.
        self.forward_sessions(&forwarded_sessions, self.application_id, callee_id)?;
        // Make the call to user code.
        let authenticated_caller_id = authenticated.then_some(self.application_id);
        let callee_context = crate::CalleeContext {
            chain_id: self.chain_id,
            authenticated_caller_id,
        };
        let raw_result = application
            .call_application(&callee_context, self, argument, forwarded_sessions)
            .await?;
        // Interprete the results of the call.
        self.try_lock_application_results()?
            .push(ApplicationResult::User(callee_id, raw_result.chain_effect));
        let sessions =
            self.make_sessions(raw_result.new_sessions, callee_id, self.application_id)?;
        let result = CallResult {
            value: raw_result.return_value,
            sessions,
        };
        Ok(result)
    }

    async fn try_call_session(
        &self,
        authenticated: bool,
        session_id: SessionId,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, Error> {
        // Load the application.
        let callee_id = session_id.application_id;
        let application = self
            .try_lock_execution_state()?
            .context()
            .extra()
            .get_user_application(callee_id)?;
        // Change the owners of forwarded sessions.
        self.forward_sessions(&forwarded_sessions, self.application_id, callee_id)?;
        // Load the session.
        let mut session_data = self.try_load_session(session_id, self.application_id)?;
        // Make the call to user code.
        let authenticated_caller_id = authenticated.then_some(self.application_id);
        let callee_context = crate::CalleeContext {
            chain_id: self.chain_id,
            authenticated_caller_id,
        };
        let raw_result = application
            .call_session(
                &callee_context,
                self,
                session_id.kind,
                &mut session_data,
                argument,
                forwarded_sessions,
            )
            .await?;
        // Interprete the results of the call.
        if raw_result.close_session {
            // Terminate the session.
            self.try_close_session(session_id, self.application_id)?;
        } else {
            // Save the session.
            self.try_save_session(session_id, self.application_id, session_data)?;
        }
        self.try_lock_application_results()?
            .push(ApplicationResult::User(callee_id, raw_result.chain_effect));
        let sessions =
            self.make_sessions(raw_result.new_sessions, callee_id, self.application_id)?;
        let result = CallResult {
            value: raw_result.return_value,
            sessions,
        };
        Ok(result)
    }
}
