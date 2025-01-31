// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! GraphQL traits for generating interfaces into applications.

use std::sync::Arc;

/// Re-exports the derive macro for [`GraphQLMutationRoot`].
pub use linera_sdk_derive::GraphQLMutationRoot;

use crate::{Service, ServiceRuntime};

/// An object associated with a GraphQL mutation root. Those are typically used to build
/// an [`async_graphql::Schema`] object.
pub trait GraphQLMutationRoot<Application>
where
    Application: Service,
{
    /// The type of the mutation root.
    type MutationRoot: async_graphql::ObjectType;

    /// Returns the mutation root of the object.
    fn mutation_root(runtime: Arc<ServiceRuntime<Application>>) -> Self::MutationRoot;
}
