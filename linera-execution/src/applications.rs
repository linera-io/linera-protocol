// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{ExecutionError, NewApplication};
use linera_base::{crypto::HashValue, data_types::EffectId};
use linera_views::{
    common::Context,
    map_view::MapView,
    views::{HashableContainerView, View, ViewError},
};
use serde::{Deserialize, Serialize};

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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UserApplicationDescription {
    /// The unique ID of the bytecode to use for the application.
    pub bytecode_id: BytecodeId,
    /// The location of the bytecode to use for the application.
    pub bytecode_location: BytecodeLocation,
    /// The unique ID of the application's creation.
    pub creation: EffectId,
    /// The argument used during application initialization.
    pub initialization_argument: Vec<u8>,
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

impl<C> ApplicationRegistryView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    /// Register a published bytecode so that it can be used by applications.
    ///
    /// Keeps track of the bytecode's location so that it can be loaded when needed.
    pub fn register_published_bytecode(&mut self, id: BytecodeId, location: BytecodeLocation) {
        self.published_bytecodes
            .insert(&id, location)
            .expect("serialization error for id");
    }

    /// Register an existing application.
    ///
    /// Keeps track of an existing application that the current chain is seeing for the first time.
    pub fn register_existing_application(
        &mut self,
        application: UserApplicationDescription,
    ) -> Result<UserApplicationId, ExecutionError> {
        let id = UserApplicationId::from(&application);
        self.known_applications.insert(&id, application)?;
        Ok(id)
    }

    /// Register a newly created application.
    pub async fn register_new_application(
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
            .ok_or(ExecutionError::UnknownBytecodeId(bytecode_id))?;

        let description = UserApplicationDescription {
            bytecode_location,
            bytecode_id,
            creation,
            initialization_argument: new_application.initialization_argument,
        };

        self.known_applications
            .insert(&new_application.id, description.clone())
            .expect("serialization error for id");

        Ok(description)
    }

    /// Retrieve an application's description.
    pub async fn describe_application(
        &mut self,
        id: UserApplicationId,
    ) -> Result<UserApplicationDescription, ExecutionError> {
        self.known_applications
            .get(&id)
            .await?
            .ok_or_else(|| ExecutionError::UnknownApplicationId(Box::new(id)))
    }
}
