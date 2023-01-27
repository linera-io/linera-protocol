use async_graphql::scalar;
use crate::worker::Notification;

// Leaves of a GraphQL schema are called `Scalars`.
// As long as a type is `Serialize`/`Deserialize`, `Scalar` can be derived
// using a declarative macro.

scalar!(Notification);