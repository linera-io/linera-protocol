use crate::data_types::ChainId;
use async_graphql::scalar;

// Leaves of a GraphQL schema are called `Scalars`.
// As long as a type is `Serialize`/`Deserialize`, `Scalar` can be derived
// using a declarative macro.

scalar!(ChainId);
