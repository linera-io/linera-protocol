// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! GraphQL traits for generating interfaces into applications.

/// Re-exports the derive macro for [`GraphQLMutationRoot`].
pub use linera_sdk_derive::GraphQLMutationRoot;

/// An object associated with a GraphQL mutation root. Those are typically used to build
/// an [`async_graphql::Schema`] object.
pub trait GraphQLMutationRoot {
    /// The type of the mutation root.
    type MutationRoot: async_graphql::ObjectType;

    /// Returns the mutation root of the object.
    fn mutation_root() -> Self::MutationRoot;
}
