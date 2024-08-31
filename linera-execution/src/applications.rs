// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};

use linera_base::{data_types::UserApplicationDescription, identifiers::UserApplicationId};
use linera_views::{
    common::Context,
    map_view::HashedMapView,
    views::{ClonableView, HashableView},
};
#[cfg(with_testing)]
use {
    linera_views::memory::{create_test_memory_context, MemoryContext},
    linera_views::views::View,
    std::collections::BTreeMap,
};

use crate::SystemExecutionError;

#[cfg(test)]
#[path = "unit_tests/applications_tests.rs"]
mod applications_tests;

#[derive(Debug, ClonableView, HashableView)]
pub struct ApplicationRegistryView<C> {
    /// The applications that are known by the chain.
    pub known_applications: HashedMapView<C, UserApplicationId, UserApplicationDescription>,
}

#[cfg(with_testing)]
#[derive(Default, Eq, PartialEq, Debug, Clone)]
pub struct ApplicationRegistry {
    pub known_applications: BTreeMap<UserApplicationId, UserApplicationDescription>,
}

impl<C> ApplicationRegistryView<C>
where
    C: Context + Clone + Send + Sync + 'static,
{
    #[cfg(with_testing)]
    pub fn import(&mut self, registry: ApplicationRegistry) -> Result<(), SystemExecutionError> {
        for (id, description) in registry.known_applications {
            self.known_applications.insert(&id, description)?;
        }
        Ok(())
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
        let description = UserApplicationDescription {
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

#[cfg(with_testing)]
impl ApplicationRegistryView<MemoryContext<()>>
where
    MemoryContext<()>: Context + Clone + Send + Sync + 'static,
{
    pub async fn new() -> Self {
        let context = create_test_memory_context();
        Self::load(context)
            .await
            .expect("Loading from memory should work")
    }
}
