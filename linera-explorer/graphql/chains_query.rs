#![allow(clippy::all, warnings)]
pub struct ChainsQuery;
pub mod chains_query {
    #![allow(dead_code)]
    use std::result::Result;
    pub const OPERATION_NAME: &str = "ChainsQuery";
    pub const QUERY: &str = "query ChainsQuery {\n  chains\n}\n";
    use super::*;
    use serde::{Deserialize, Serialize};
    #[allow(dead_code)]
    type Boolean = bool;
    #[allow(dead_code)]
    type Float = f64;
    #[allow(dead_code)]
    type Int = i64;
    #[allow(dead_code)]
    type ID = String;
    type ChainId = super::ChainId;
    #[derive(Serialize)]
    pub struct Variables;
    #[derive(Deserialize)]
    pub struct ResponseData {
        pub chains: Vec<ChainId>,
    }
}
impl graphql_client::GraphQLQuery for ChainsQuery {
    type Variables = chains_query::Variables;
    type ResponseData = chains_query::ResponseData;
    fn build_query(variables: Self::Variables) -> ::graphql_client::QueryBody<Self::Variables> {
        graphql_client::QueryBody {
            variables,
            query: chains_query::QUERY,
            operation_name: chains_query::OPERATION_NAME,
        }
    }
}
