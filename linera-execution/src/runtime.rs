// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    execution::{ExecutionStateView, ExecutionStateViewContext},
    ApplicationResult, CallResult, ExecutionRuntimeContext, NewSession, QueryableStorage,
    ReadableStorage, SessionId, WritableStorage,
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

/// Runtime data tracked during the execution of a transaction.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionRuntime<'a, C, const WRITABLE: bool> {
    /// The current chain ID.
    chain_id: ChainId,
    /// The current stack of application IDs.
    application_ids: Arc<Mutex<&'a mut Vec<ApplicationId>>>,
    /// The storage view on the execution state.
    execution_state: Arc<Mutex<&'a mut ExecutionStateView<C>>>,
    /// All the sessions and their IDs.
    session_manager: Arc<Mutex<&'a mut SessionManager>>,
    /// Track active (i.e. locked) applications for which re-entrancy is disallowed.
    active_user_states: Arc<Mutex<ActiveUserStates<C>>>,
    /// Track active (i.e. locked) sessions for which re-entrancy is disallowed.
    active_sessions: Arc<Mutex<ActiveSessions>>,
    /// Accumulate the externally visible results (e.g. cross-chain messages) of applications.
    application_results: Arc<Mutex<&'a mut Vec<ApplicationResult>>>,
}

type ActiveUserStates<C> = BTreeMap<ApplicationId, OwnedMutexGuard<RegisterView<C, Vec<u8>>>>;

type ActiveSessions = BTreeMap<SessionId, OwnedMutexGuard<SessionState>>;

#[derive(Debug, Clone, Default)]
pub(crate) struct SessionManager {
    /// Track the next session index to be used for each application.
    pub(crate) counters: BTreeMap<ApplicationId, u64>,
    /// Track the current state (owner and data) of each session.
    pub(crate) states: BTreeMap<SessionId, Arc<Mutex<SessionState>>>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SessionState {
    /// Track which application can call into the session.
    owner: ApplicationId,
    /// The internal state of the session.
    data: Vec<u8>,
}

impl<'a, C, const W: bool> ExecutionRuntime<'a, C, W> {
    pub(crate) fn new(
        chain_id: ChainId,
        application_ids: &'a mut Vec<ApplicationId>,
        execution_state: &'a mut ExecutionStateView<C>,
        session_manager: &'a mut SessionManager,
        application_results: &'a mut Vec<ApplicationResult>,
    ) -> Self {
        Self {
            chain_id,
            application_ids: Arc::new(Mutex::new(application_ids)),
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
{
    fn application_ids_mut(&self) -> MutexGuard<'_, &'a mut Vec<ApplicationId>> {
        self.application_ids
            .try_lock()
            .expect("single-threaded execution should not lock `application_ids`")
    }

    fn execution_state_mut(&self) -> MutexGuard<'_, &'a mut ExecutionStateView<C>> {
        self.execution_state
            .try_lock()
            .expect("single-threaded execution should not lock `execution_state`")
    }

    fn session_manager_mut(&self) -> MutexGuard<'_, &'a mut SessionManager> {
        self.session_manager
            .try_lock()
            .expect("single-threaded execution should not lock `session_manager`")
    }

    fn active_user_states_mut(&self) -> MutexGuard<'_, ActiveUserStates<C>> {
        self.active_user_states
            .try_lock()
            .expect("single-threaded execution should not lock `active_user_states`")
    }

    fn active_sessions_mut(&self) -> MutexGuard<'_, ActiveSessions> {
        self.active_sessions
            .try_lock()
            .expect("single-threaded execution should not lock `active_sessions`")
    }

    fn application_results_mut(&self) -> MutexGuard<'_, &'a mut Vec<ApplicationResult>> {
        self.application_results
            .try_lock()
            .expect("single-threaded execution should not lock `application_results`")
    }

    fn forward_sessions(
        &self,
        session_ids: &[SessionId],
        from_id: ApplicationId,
        to_id: ApplicationId,
    ) -> Result<(), Error> {
        let states = &self.session_manager_mut().states;
        for id in session_ids {
            let mut state = states
                .get(id)
                .ok_or(Error::InvalidSession)?
                .clone()
                .try_lock_owned()
                .map_err(|_| Error::SessionIsInUse)?;
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
        let mut manager = self.session_manager_mut();
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
            .session_manager_mut()
            .states
            .get(&session_id)
            .ok_or(Error::InvalidSession)?
            .clone()
            .try_lock_owned()
            .map_err(|_| Error::SessionIsInUse)?;
        let state = guard.data.clone();
        // Verify ownership.
        ensure!(guard.owner == application_id, Error::InvalidSessionOwner);
        // Remember the guard. This will prevent reentrancy.
        self.active_sessions_mut().insert(session_id, guard);
        Ok(state)
    }

    fn try_save_session(
        &self,
        session_id: SessionId,
        application_id: ApplicationId,
        state: Vec<u8>,
    ) -> Result<(), Error> {
        // Remove the guard.
        if let Some(mut guard) = self.active_sessions_mut().remove(&session_id) {
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
        if let Some(guard) = self.active_sessions_mut().remove(&session_id) {
            // Verify ownership.
            ensure!(guard.owner == application_id, Error::InvalidSessionOwner);
            // Delete the session.
            self.active_sessions_mut().remove(&session_id);
        }
        Ok(())
    }
}

#[async_trait]
impl<'a, C, const W: bool> ReadableStorage for ExecutionRuntime<'a, C, W>
where
    C: ExecutionStateViewContext,
    C::Extra: ExecutionRuntimeContext,
{
    fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    fn application_id(&self) -> ApplicationId {
        *self
            .application_ids_mut()
            .last()
            .expect("at least one application id should be present in the stack")
    }

    async fn try_read_system_balance(&self) -> Result<crate::system::Balance, Error> {
        let value = *self.execution_state_mut().system.balance.get();
        Ok(value)
    }

    async fn try_read_my_state(&self) -> Result<Vec<u8>, Error> {
        let state = self
            .execution_state_mut()
            .users
            .try_load_entry(self.application_id())
            .await
            .map_err(|_| {
                // FIXME(#152): This remapping is too coarse as the error could be network-related.
                Error::ApplicationIsInUse
            })?
            .get()
            .to_vec();
        Ok(state)
    }
}

#[async_trait]
impl<'a, C> QueryableStorage for ExecutionRuntime<'a, C, false>
where
    C: ExecutionStateViewContext,
    C::Extra: ExecutionRuntimeContext,
{
    /// Note that queries are not available from writable contexts.
    async fn try_query_application(
        &self,
        callee_id: ApplicationId,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error> {
        // Load the application.
        let application = self
            .execution_state_mut()
            .context()
            .extra()
            .get_user_application(callee_id)?;
        // Make the call to user code.
        let query_context = crate::QueryContext {
            chain_id: self.chain_id,
        };
        self.application_ids_mut().push(callee_id);
        let value = application
            .query_application(&query_context, self, argument)
            .await?;
        self.application_ids_mut().pop();
        Ok(value)
    }
}

#[async_trait]
impl<'a, C> WritableStorage for ExecutionRuntime<'a, C, true>
where
    C: ExecutionStateViewContext,
    C::Extra: ExecutionRuntimeContext,
{
    async fn try_load_my_state(&self) -> Result<Vec<u8>, Error> {
        let view = self
            .execution_state_mut()
            .users
            .try_load_entry(self.application_id())
            .await
            .map_err(|_| {
                // FIXME(#152): This remapping is too coarse as the error could be network-related.
                Error::ApplicationIsInUse
            })?;
        let state = view.get().to_vec();
        // Remember the view. This will prevent reentrancy.
        self.active_user_states_mut()
            .insert(self.application_id(), view);
        Ok(state)
    }

    async fn try_save_my_state(&self, state: Vec<u8>) -> Result<(), Error> {
        if let Some(mut view) = self.active_user_states_mut().remove(&self.application_id()) {
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
            .execution_state_mut()
            .context()
            .extra()
            .get_user_application(callee_id)?;
        // Change the owners of forwarded sessions.
        self.forward_sessions(&forwarded_sessions, self.application_id(), callee_id)?;
        // Make the call to user code.
        let authenticated_caller_id = authenticated.then_some(self.application_id());
        let callee_context = crate::CalleeContext {
            chain_id: self.chain_id,
            authenticated_caller_id,
        };
        self.application_ids_mut().push(callee_id);
        let raw_result = application
            .call_application(&callee_context, self, argument, forwarded_sessions)
            .await?;
        self.application_ids_mut().pop();
        // Interprete the results of the call.
        self.application_results_mut().push(ApplicationResult::User(
            callee_id,
            raw_result.application_result,
        ));
        let sessions =
            self.make_sessions(raw_result.create_sessions, callee_id, self.application_id())?;
        let result = CallResult {
            value: raw_result.value,
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
            .execution_state_mut()
            .context()
            .extra()
            .get_user_application(callee_id)?;
        // Change the owners of forwarded sessions.
        self.forward_sessions(&forwarded_sessions, self.application_id(), callee_id)?;
        // Load the session.
        let mut session_data = self.try_load_session(session_id, self.application_id())?;
        // Make the call to user code.
        let authenticated_caller_id = authenticated.then_some(self.application_id());
        let callee_context = crate::CalleeContext {
            chain_id: self.chain_id,
            authenticated_caller_id,
        };
        self.application_ids_mut().push(callee_id);
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
        self.application_ids_mut().pop();
        // Interprete the results of the call.
        if raw_result.close_session {
            // Terminate the session.
            self.try_close_session(session_id, self.application_id())?;
        } else {
            // Save the session.
            self.try_save_session(session_id, self.application_id(), session_data)?;
        }
        self.application_results_mut().push(ApplicationResult::User(
            callee_id,
            raw_result.application_result,
        ));
        let sessions =
            self.make_sessions(raw_result.create_sessions, callee_id, self.application_id())?;
        let result = CallResult {
            value: raw_result.value,
            sessions,
        };
        Ok(result)
    }
}
