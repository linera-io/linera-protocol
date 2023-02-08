// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::SystemExecutionError;
use linera_base::{crypto::CryptoHash, data_types::EffectId};
use linera_views::{
    common::Context,
    map_view::MapView,
    views::{HashableContainerView, View, ViewError},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt};

#[cfg(any(test, feature = "test"))]
use {
    linera_views::memory::MemoryContext, std::collections::BTreeMap, std::sync::Arc,
    tokio::sync::Mutex,
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

/// A unique identifier for a user application.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub struct UserApplicationId {
    /// The bytecode to use for the application.
    pub bytecode_id: BytecodeId,
    /// The unique ID of the application's creation.
    pub creation: EffectId,
}

/// Description of the necessary information to run a user application.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ApplicationDescription {
    /// A special reference to the system application.
    System,
    /// A reference to a user application.
    User(UserApplicationDescription),
}

/// Description of the necessary information to run a user application.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Deserialize, Eq, PartialEq, Hash, Serialize)]
pub struct UserApplicationDescription {
    /// The unique ID of the bytecode to use for the application.
    pub bytecode_id: BytecodeId,
    /// The location of the bytecode to use for the application.
    pub bytecode_location: BytecodeLocation,
    /// The unique ID of the application's creation.
    pub creation: EffectId,
    /// The argument used during application initialization.
    #[serde(with = "serde_bytes")]
    pub initialization_argument: Vec<u8>,
    /// Required dependencies.
    pub required_application_ids: Vec<UserApplicationId>,
}

impl fmt::Debug for UserApplicationDescription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UserApplicationDescription")
            .field("bytecode_id", &self.bytecode_id)
            .field("bytecode_location", &self.bytecode_location)
            .field("creation", &self.creation)
            .field(
                "initialization_argument",
                &hex::encode(&self.initialization_argument),
            )
            .field("required_application_ids", &self.required_application_ids)
            .finish()
    }
}

impl From<EffectId> for BytecodeId {
    fn from(effect_id: EffectId) -> Self {
        BytecodeId(effect_id)
    }
}

impl From<&ApplicationDescription> for ApplicationId {
    fn from(description: &ApplicationDescription) -> Self {
        match description {
            ApplicationDescription::System => ApplicationId::System,
            ApplicationDescription::User(application) => ApplicationId::User(application.into()),
        }
    }
}

impl From<&UserApplicationDescription> for UserApplicationId {
    fn from(description: &UserApplicationDescription) -> Self {
        UserApplicationId {
            bytecode_id: description.bytecode_id,
            creation: description.creation,
        }
    }
}

/// A unique identifier for an application bytecode.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BytecodeId(pub EffectId);

/// A reference to where the application bytecode is stored.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct BytecodeLocation {
    /// The certificate that published the bytecode.
    pub certificate_hash: CryptoHash,
    /// The index in the certificate of the operation that published the bytecode.
    pub operation_index: usize,
}

#[derive(Debug, HashableContainerView)]
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

    /// Register a published bytecode so that it can be used by applications.
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
        &mut self,
    ) -> Result<Vec<(BytecodeId, BytecodeLocation)>, SystemExecutionError> {
        let mut locations = Vec::new();
        for id in self.published_bytecodes.indices().await? {
            if let Some(location) = self.published_bytecodes.get(&id).await? {
                locations.push((id, location));
            }
        }
        Ok(locations)
    }

    /// Register an existing application.
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

    /// Register a newly created application.
    pub async fn create_application(
        &mut self,
        application_id: UserApplicationId,
        initialization_argument: Vec<u8>,
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
            creation,
            required_application_ids,
            initialization_argument,
        };
        self.known_applications
            .insert(&application_id, description)?;
        Ok(())
    }

    /// Retrieve an application's description.
    pub async fn describe_application(
        &mut self,
        id: UserApplicationId,
    ) -> Result<UserApplicationDescription, SystemExecutionError> {
        self.known_applications
            .get(&id)
            .await?
            .ok_or_else(|| SystemExecutionError::UnknownApplicationId(Box::new(id)))
    }

    /// Retrieve the recursive dependencies of an application and apply a topological
    /// sort.
    pub async fn find_dependencies(
        &mut self,
        id: UserApplicationId,
    ) -> Result<Vec<UserApplicationId>, SystemExecutionError> {
        // What we return at the end.
        let mut result = Vec::new();
        // The calling stack.
        let mut stack = vec![id];
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
            // 1. Mark it so that its dependencies are no longer push to the stack.
            seen.insert(id);
            // 2. Schedule all the (yet unseen) dependencies, then this entry for a second visit.
            stack.push(id);
            let app = self
                .known_applications
                .get(&id)
                .await?
                .ok_or_else(|| SystemExecutionError::UnknownApplicationId(Box::new(id)))?;
            for child in app.required_application_ids.iter().rev() {
                if !seen.contains(child) {
                    stack.push(*child);
                }
            }
        }
        Ok(result)
    }

    /// Retrieve an application's description preceded by its recursive dependencies.
    pub async fn describe_application_with_dependencies(
        &mut self,
        id: UserApplicationId,
    ) -> Result<Vec<UserApplicationDescription>, SystemExecutionError> {
        let ids = self.find_dependencies(id).await?;
        let mut result = Vec::new();
        for id in ids {
            let description = self
                .known_applications
                .get(&id)
                .await?
                .ok_or_else(|| SystemExecutionError::UnknownApplicationId(Box::new(id)))?;
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
        let guard = Arc::new(Mutex::new(BTreeMap::new())).lock_owned().await;
        let context = MemoryContext::new(guard, ());
        Self::load(context)
            .await
            .expect("Loading from memory should work")
    }
}
