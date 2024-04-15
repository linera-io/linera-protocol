use async_graphql::{Request, Response};
use linera_sdk::base::{ContractAbi, ServiceAbi};

pub struct LlmAbi;

impl ContractAbi for LlmAbi {
    type Parameters = ();
    type InitializationArgument = ();
    type Operation = ();
    type Message = ();
    type ApplicationCall = ();
    type SessionCall = ();
    type SessionState = ();
    type Response = ();
}

impl ServiceAbi for LlmAbi {
    type Parameters = ();
    type Query = Request;
    type QueryResponse = Response;
}
