// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::SystemExecutionError;
use custom_debug_derive::Debug;
use linera_base::{
    crypto::CryptoHash,
    hex_debug,
    identifiers::{BytecodeId, MessageId},
};
use linera_views::{
    common::Context,
    map_view::MapView,
    views::{HashableView, ViewError},
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[cfg(any(test, feature = "test"))]
use {
    async_lock::Mutex,
    linera_views::memory::{MemoryContext, TEST_MEMORY_MAX_STREAM_QUERIES},
    linera_views::views::View,
    std::collections::BTreeMap,
    std::sync::Arc,
};

#[cfg(test)]
#[path = "unit_tests/applications_tests.rs"]
mod applications_tests;

/// A unique identifier for an application.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub enum ApplicationId {
    /// The system application.
    System,
    /// A user application.
    User(UserApplicationId),
}

impl ApplicationId {
    pub fn user_application_id(&self) -> Option<&UserApplicationId> {
        if let ApplicationId::User(app_id) = self {
            Some(app_id)
        } else {
            None
        }
    }
}

/// Alias for `linera_base::identifiers::ApplicationId`. Use this alias in the core
/// protocol where the distinction with the more general enum [`ApplicationId`] matters.
pub type UserApplicationId<A = ()> = linera_base::identifiers::ApplicationId<A>;

/// Description of the necessary information to run a user application.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Hash, Serialize)]
pub struct UserApplicationDescription {
    /// The unique ID of the bytecode to use for the application.
    pub bytecode_id: BytecodeId,
    /// The location of the bytecode to use for the application.
    pub bytecode_location: BytecodeLocation,
    /// The unique ID of the application's creation.
    pub creation: MessageId,
    /// The parameters of the application.
    #[serde(with = "serde_bytes")]
    #[debug(with = "hex_debug")]
    pub parameters: Vec<u8>,
    /// Required dependencies.
    pub required_application_ids: Vec<UserApplicationId>,
}

impl From<&UserApplicationDescription> for UserApplicationId {
    fn from(description: &UserApplicationDescription) -> Self {
        UserApplicationId {
            bytecode_id: description.bytecode_id,
            creation: description.creation,
        }
    }
}

impl From<UserApplicationId> for ApplicationId {
    fn from(user_application_id: UserApplicationId) -> Self {
        ApplicationId::User(user_application_id)
    }
}

/// A reference to where the application bytecode is stored.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct BytecodeLocation {
    /// The certificate that published the bytecode.
    pub certificate_hash: CryptoHash,
    /// The index in the certificate of the operation that published the bytecode (not the message!).
    pub operation_index: u32,
}

#[derive(Debug, HashableView)]
pub struct ApplicationRegistryView<C> {
    /// The application bytecodes that have been published.
    pub published_bytecodes: MapView<C, BytecodeId, BytecodeLocation>,
    /// The applications that are known by the chain.
    pub known_applications: MapView<C, UserApplicationId, UserApplicationDescription>,
}

#[cfg(any(test, feature = "test"))]
#[derive(Default, Eq, PartialEq, Debug, Clone)]
pub struct ApplicationRegistry {
    pub published_bytecodes: BTreeMap<BytecodeId, BytecodeLocation>,
    pub known_applications: BTreeMap<UserApplicationId, UserApplicationDescription>,
}

impl<C> ApplicationRegistryView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    #[cfg(any(test, feature = "test"))]
    pub fn import(&mut self, registry: ApplicationRegistry) -> Result<(), SystemExecutionError> {
        for (id, location) in registry.published_bytecodes {
            self.published_bytecodes.insert(&id, location)?;
        }
        for (id, description) in registry.known_applications {
            self.known_applications.insert(&id, description)?;
        }
        Ok(())
    }

    /// Registers a published bytecode so that it can be used by applications.
    ///
    /// Keeps track of the bytecode's location so that it can be loaded when needed.
    pub fn register_published_bytecode(
        &mut self,
        id: BytecodeId,
        location: BytecodeLocation,
    ) -> Result<(), SystemExecutionError> {
        self.published_bytecodes.insert(&id, location)?;
        Ok(())
    }

    /// Returns all the known locations of published bytecode.
    pub async fn bytecode_locations(
        &self,
    ) -> Result<Vec<(BytecodeId, BytecodeLocation)>, SystemExecutionError> {
        let mut locations = Vec::new();
        self.published_bytecodes
            .for_each_index_value(|id, location| {
                locations.push((id, location));
                Ok(())
            })
            .await?;
        Ok(locations)
    }

    /// Returns the location of published bytecode with the given ID.
    pub async fn bytecode_location_for(
        &self,
        id: &BytecodeId,
    ) -> Result<Option<BytecodeLocation>, SystemExecutionError> {
        Ok(self.published_bytecodes.get(id).await?)
    }

    /// Registers an existing application.
    ///
    /// Keeps track of an existing application that the current chain is seeing for the first time.
    pub async fn register_application(
        &mut self,
        application: UserApplicationDescription,
    ) -> Result<UserApplicationId, SystemExecutionError> {
        // Make sure that referenced applications ids have been registered.
        for required_id in &application.required_application_ids {
            self.describe_application(*required_id).await?;
        }
        let id = UserApplicationId::from(&application);
        self.known_applications.insert(&id, application)?;
        Ok(id)
    }

    /// Registers a newly created application.
    pub async fn register_new_application(
        &mut self,
        application_id: UserApplicationId,
        parameters: Vec<u8>,
        required_application_ids: Vec<UserApplicationId>,
    ) -> Result<(), SystemExecutionError> {
        // Make sure that referenced applications ids have been registered.
        for required_id in &required_application_ids {
            self.describe_application(*required_id).await?;
        }
        // Create description and register it.
        let UserApplicationId {
            bytecode_id,
            creation,
        } = application_id;
        let bytecode_location = self
            .published_bytecodes
            .get(&bytecode_id)
            .await?
            .ok_or(SystemExecutionError::UnknownBytecodeId(bytecode_id))?;
        let description = UserApplicationDescription {
            bytecode_location,
            bytecode_id,
            parameters,
            creation,
            required_application_ids,
        };
        self.known_applications
            .insert(&application_id, description)?;
        Ok(())
    }

    /// Retrieves an application's description.
    pub async fn describe_application(
        &self,
        id: UserApplicationId,
    ) -> Result<UserApplicationDescription, SystemExecutionError> {
        self.known_applications
            .get(&id)
            .await?
            .ok_or_else(|| SystemExecutionError::UnknownApplicationId(Box::new(id)))
    }

    /// Retrieves the recursive dependencies of applications and apply a topological sort.
    pub async fn find_dependencies(
        &self,
        mut stack: Vec<UserApplicationId>,
        registered_apps: &HashMap<UserApplicationId, UserApplicationDescription>,
    ) -> Result<Vec<UserApplicationId>, SystemExecutionError> {
        // What we return at the end.
        let mut result = Vec::new();
        // The entries already inserted in `result`.
        let mut sorted = HashSet::new();
        // The entries for which dependencies have already been pushed once to the stack.
        let mut seen = HashSet::new();

        while let Some(id) = stack.pop() {
            if sorted.contains(&id) {
                continue;
            }
            if seen.contains(&id) {
                // Second time we see this entry. It was last pushed just before its
                // dependencies -- which are now fully sorted.
                sorted.insert(id);
                result.push(id);
                continue;
            }
            // First time we see this entry:
            // 1. Mark it so that its dependencies are no longer pushed to the stack.
            seen.insert(id);
            // 2. Schedule all the (yet unseen) dependencies, then this entry for a second visit.
            stack.push(id);
            let app = if let Some(app) = registered_apps.get(&id) {
                app.clone()
            } else {
                self.describe_application(id).await?
            };
            for child in app.required_application_ids.iter().rev() {
                if !seen.contains(child) {
                    stack.push(*child);
                }
            }
        }
        Ok(result)
    }

    /// Retrieves applications' descriptions preceded by their recursive dependencies.
    pub async fn describe_applications_with_dependencies(
        &self,
        ids: Vec<UserApplicationId>,
        extra_registered_apps: &HashMap<UserApplicationId, UserApplicationDescription>,
    ) -> Result<Vec<UserApplicationDescription>, SystemExecutionError> {
        let ids_with_deps = self.find_dependencies(ids, extra_registered_apps).await?;
        let mut result = Vec::new();
        for id in ids_with_deps {
            let description = if let Some(description) = extra_registered_apps.get(&id) {
                description.clone()
            } else {
                self.describe_application(id).await?
            };
            result.push(description);
        }
        Ok(result)
    }
}

#[cfg(any(test, feature = "test"))]
impl ApplicationRegistryView<MemoryContext<()>>
where
    MemoryContext<()>: Context + Clone + Send + Sync + 'static,
    ViewError: From<<MemoryContext<()> as linera_views::common::Context>::Error>,
{
    pub async fn new() -> Self {
        let guard = Arc::new(Mutex::new(BTreeMap::new())).lock_arc().await;
        let context = MemoryContext::new(guard, TEST_MEMORY_MAX_STREAM_QUERIES, ());
        Self::load(context)
            .await
            .expect("Loading from memory should work")
    }
}
