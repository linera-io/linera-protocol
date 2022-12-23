// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    execution::ExecutionStateView, ApplicationId, ApplicationRegistryView,
    ApplicationStateNotLocked, CallResult, ExecutionError, ExecutionResult,
    ExecutionRuntimeContext, NewSession, QueryableStorage, ReadableStorage, SessionId,
    UserApplicationCode, WritableStorage,
};
use async_trait::async_trait;
use linera_base::{ensure, messages::ChainId};
use linera_views::{
    common::Context,
    register_view::RegisterView,
    views::{View, ViewError},
};
use std::{
    collections::{btree_map, BTreeMap},
    ops::DerefMut,
    sync::Arc,
};
use tokio::sync::{Mutex, MutexGuard, OwnedMutexGuard};

/// Runtime data tracked during the execution of a transaction.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionRuntime<'a, C, const WRITABLE: bool> {
    /// The current chain ID.
    chain_id: ChainId,
    /// The registry of applications known by the current chain.
    application_registry: Arc<Mutex<&'a mut ApplicationRegistryView<C>>>,
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
    execution_results: Arc<Mutex<&'a mut Vec<ExecutionResult>>>,
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

#[derive(Debug, Clone)]
pub(crate) struct SessionState {
    /// Track which application can call into the session.
    owner: ApplicationId,
    /// The internal state of the session.
    data: Vec<u8>,
}

impl<'a, C, const W: bool> ExecutionRuntime<'a, C, W>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    pub(crate) fn new(
        chain_id: ChainId,
        application_registry: &'a mut ApplicationRegistryView<C>,
        application_ids: &'a mut Vec<ApplicationId>,
        execution_state: &'a mut ExecutionStateView<C>,
        session_manager: &'a mut SessionManager,
        execution_results: &'a mut Vec<ExecutionResult>,
    ) -> Self {
        assert_eq!(chain_id, execution_state.context().extra().chain_id());
        Self {
            chain_id,
            application_registry: Arc::new(Mutex::new(application_registry)),
            application_ids: Arc::new(Mutex::new(application_ids)),
            execution_state: Arc::new(Mutex::new(execution_state)),
            session_manager: Arc::new(Mutex::new(session_manager)),
            active_user_states: Arc::default(),
            active_sessions: Arc::default(),
            execution_results: Arc::new(Mutex::new(execution_results)),
        }
    }

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

    fn execution_results_mut(&self) -> MutexGuard<'_, &'a mut Vec<ExecutionResult>> {
        self.execution_results
            .try_lock()
            .expect("single-threaded execution should not lock `execution_results`")
    }

    async fn load_application(
        &self,
        id: ApplicationId,
    ) -> Result<UserApplicationCode, ExecutionError> {
        let mut registry = self
            .application_registry
            .try_lock()
            .expect("single-threaded execution should not lock `application_registry`");
        let description = registry.describe_application(id).await?;
        self.execution_state_mut()
            .context()
            .extra()
            .get_user_application(&description)
            .await
    }

    fn forward_sessions(
        &self,
        session_ids: &[SessionId],
        from_id: ApplicationId,
        to_id: ApplicationId,
    ) -> Result<(), ExecutionError> {
        let states = &self.session_manager_mut().states;
        for id in session_ids {
            let mut state = states
                .get(id)
                .ok_or(ExecutionError::InvalidSession)?
                .clone()
                .try_lock_owned()
                .map_err(|_| ExecutionError::SessionIsInUse)?;
            // Verify ownership.
            ensure!(state.owner == from_id, ExecutionError::InvalidSessionOwner);
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
    ) -> Vec<SessionId> {
        let mut manager = self.session_manager_mut();
        let manager = manager.deref_mut();
        let states = &mut manager.states;
        let counter = manager.counters.entry(creator_id).or_default();
        let mut session_ids = Vec::new();
        for session in new_sessions {
            let id = SessionId {
                application_id: creator_id,
                kind: session.kind,
                index: *counter,
            };
            *counter += 1;
            session_ids.push(id);
            let state = SessionState {
                owner: receiver_id,
                data: session.data,
            };
            states.insert(id, Arc::new(Mutex::new(state)));
        }
        session_ids
    }

    fn try_load_session(
        &self,
        session_id: SessionId,
        application_id: ApplicationId,
    ) -> Result<Vec<u8>, ExecutionError> {
        let guard = self
            .session_manager_mut()
            .states
            .get(&session_id)
            .ok_or(ExecutionError::InvalidSession)?
            .clone()
            .try_lock_owned()
            .map_err(|_| ExecutionError::SessionIsInUse)?;
        let state = guard.data.clone();
        // Verify ownership.
        ensure!(
            guard.owner == application_id,
            ExecutionError::InvalidSessionOwner
        );
        // Remember the guard. This will prevent reentrancy.
        self.active_sessions_mut().insert(session_id, guard);
        Ok(state)
    }

    fn try_save_session(
        &self,
        session_id: SessionId,
        application_id: ApplicationId,
        state: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        // Remove the guard.
        if let btree_map::Entry::Occupied(mut guard) = self.active_sessions_mut().entry(session_id)
        {
            // Verify ownership.
            ensure!(
                guard.get().owner == application_id,
                ExecutionError::InvalidSessionOwner
            );
            // Save the state and unlock the session for future calls.
            guard.get_mut().data = state;
            // Remove the entry from the set of active sessions.
            guard.remove();
        }
        Ok(())
    }

    fn try_close_session(
        &self,
        session_id: SessionId,
        application_id: ApplicationId,
    ) -> Result<(), ExecutionError> {
        if let btree_map::Entry::Occupied(guard) = self.active_sessions_mut().entry(session_id) {
            // Verify ownership.
            ensure!(
                guard.get().owner == application_id,
                ExecutionError::InvalidSessionOwner
            );
            // Remove the entry from the set of active sessions.
            guard.remove();
            // Delete the session entirely.
            self.session_manager_mut()
                .states
                .remove(&session_id)
                .ok_or(ExecutionError::InvalidSession)?;
        }
        Ok(())
    }
}

#[async_trait]
impl<'a, C, const W: bool> ReadableStorage for ExecutionRuntime<'a, C, W>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
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

    fn read_system_balance(&self) -> crate::system::Balance {
        *self.execution_state_mut().system.balance.get()
    }

    async fn try_read_my_state(&self) -> Result<Vec<u8>, ExecutionError> {
        let state = self
            .execution_state_mut()
            .users
            .try_load_entry(self.application_id())
            .await?
            .get()
            .to_vec();
        Ok(state)
    }
}

#[async_trait]
impl<'a, C> QueryableStorage for ExecutionRuntime<'a, C, false>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    /// Note that queries are not available from writable contexts.
    async fn try_query_application(
        &self,
        queried_id: ApplicationId,
        argument: &[u8],
    ) -> Result<Vec<u8>, ExecutionError> {
        // Load the application.
        let application = self.load_application(queried_id).await?;
        // Make the call to user code.
        let query_context = crate::QueryContext {
            chain_id: self.chain_id,
        };
        self.application_ids_mut().push(queried_id);
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
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    async fn try_read_and_lock_my_state(&self) -> Result<Vec<u8>, ExecutionError> {
        let view = self
            .execution_state_mut()
            .users
            .try_load_entry(self.application_id())
            .await?;
        let state = view.get().to_vec();
        // Remember the view. This will prevent reentrancy.
        self.active_user_states_mut()
            .insert(self.application_id(), view);
        Ok(state)
    }

    fn save_and_unlock_my_state(&self, state: Vec<u8>) -> Result<(), ApplicationStateNotLocked> {
        // Make the view available again.
        match self.active_user_states_mut().remove(&self.application_id()) {
            Some(mut view) => {
                // Set the state.
                view.set(state);
                Ok(())
            }
            None => Err(ApplicationStateNotLocked),
        }
    }

    fn unlock_my_state(&self) {
        self.active_user_states_mut().remove(&self.application_id());
    }

    async fn try_call_application(
        &self,
        authenticated: bool,
        callee_id: ApplicationId,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError> {
        // Load the application.
        let application = self.load_application(callee_id).await?;
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
        // Interpret the results of the call.
        self.execution_results_mut().push(ExecutionResult::User(
            callee_id,
            raw_result.execution_result,
        ));
        let sessions =
            self.make_sessions(raw_result.create_sessions, callee_id, self.application_id());
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
    ) -> Result<CallResult, ExecutionError> {
        // Load the application.
        let callee_id = session_id.application_id;
        let application = self.load_application(callee_id).await?;
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
        // Interpret the results of the call.
        if raw_result.close_session {
            // Terminate the session.
            self.try_close_session(session_id, self.application_id())?;
        } else {
            // Save the session.
            self.try_save_session(session_id, self.application_id(), session_data)?;
        }
        let inner_result = raw_result.inner;
        self.execution_results_mut().push(ExecutionResult::User(
            callee_id,
            inner_result.execution_result,
        ));
        let sessions = self.make_sessions(
            inner_result.create_sessions,
            callee_id,
            self.application_id(),
        );
        let result = CallResult {
            value: inner_result.value,
            sessions,
        };
        Ok(result)
    }
}
