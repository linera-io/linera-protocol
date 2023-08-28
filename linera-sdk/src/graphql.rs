// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! GraphQL traits for generating interfaces into applications.

/// A trait which is derived by the `MutationRoot` proc macro.
pub trait GraphQLMutationRoot {
    /// The associated `MutationRoot` type which is code-generated.
    type MutationRoot: async_graphql::ObjectType;

    /// Returns the `MutationRoot` for a given operation.
    fn mutation_root() -> Self::MutationRoot;
}
