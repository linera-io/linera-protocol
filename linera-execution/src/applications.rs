// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{ExecutionError, NewApplication, SystemExecutionError};
use linera_base::{crypto::HashValue, data_types::EffectId};
use linera_views::{
    common::Context,
    map_view::MapView,
    views::{HashableContainerView, View, ViewError},
};
use serde::{Deserialize, Serialize};

#[cfg(any(test, feature = "test"))]
use std::collections::BTreeMap;

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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Hash, Serialize)]
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
    pub certificate_hash: HashValue,
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

    /// Register an existing application.
    ///
    /// Keeps track of an existing application that the current chain is seeing for the first time.
    pub fn declare_application(
        &mut self,
        application: UserApplicationDescription,
    ) -> Result<UserApplicationId, SystemExecutionError> {
        let id = UserApplicationId::from(&application);
        self.known_applications.insert(&id, application)?;
        Ok(id)
    }

    /// Register a newly created application.
    pub async fn create_application(
        &mut self,
        new_application: NewApplication,
    ) -> Result<UserApplicationDescription, ExecutionError> {
        let UserApplicationId {
            bytecode_id,
            creation,
        } = new_application.id;

        let bytecode_location = self
            .published_bytecodes
            .get(&bytecode_id)
            .await?
            .ok_or(SystemExecutionError::UnknownBytecodeId(bytecode_id))?;

        let description = UserApplicationDescription {
            bytecode_location,
            bytecode_id,
            creation,
            required_application_ids: new_application.required_application_ids,
            initialization_argument: new_application.initialization_argument,
        };

        self.known_applications
            .insert(&new_application.id, description.clone())?;

        Ok(description)
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
}
