#![allow(clippy::all, warnings)]
pub struct BlockQuery;
pub mod block_query {
    #![allow(dead_code)]
    use std::result::Result;
    pub const OPERATION_NAME: &str = "BlockQuery";
    pub const QUERY : & str = "query BlockQuery($hash: CryptoHash, $chainId: ChainId) {\n  block(hash: $hash, chainId: $chainId) {\n    hash\n    value {\n      status\n      round\n      executedBlock {\n        block {\n          chainId\n          # epoch\n          height\n          timestamp\n          authenticatedSigner\n          previousBlockHash\n          # incomingMessages {\n          #   origin\n          #   event\n          # }\n          # operations\n        }\n        effects {\n          destination\n          authenticatedSigner\n          # effect\n        }\n        stateHash\n      }\n    }\n  }\n}\n" ;
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
    type BlockHeight = super::BlockHeight;
    type ChainId = super::ChainId;
    type CryptoHash = super::CryptoHash;
    type Destination = super::Destination;
    type Owner = super::Owner;
    type RoundNumber = super::RoundNumber;
    type Timestamp = super::Timestamp;
    #[derive(Serialize)]
    pub struct Variables {
        pub hash: Option<CryptoHash>,
        #[serde(rename = "chainId")]
        pub chain_id: Option<ChainId>,
    }
    impl Variables {}
    #[derive(Deserialize)]
    pub struct ResponseData {
        pub block: Option<BlockQueryBlock>,
    }
    #[derive(Deserialize)]
    pub struct BlockQueryBlock {
        pub hash: CryptoHash,
        pub value: BlockQueryBlockValue,
    }
    #[derive(Deserialize)]
    pub struct BlockQueryBlockValue {
        pub status: String,
        pub round: RoundNumber,
        #[serde(rename = "executedBlock")]
        pub executed_block: BlockQueryBlockValueExecutedBlock,
    }
    #[derive(Deserialize)]
    pub struct BlockQueryBlockValueExecutedBlock {
        pub block: BlockQueryBlockValueExecutedBlockBlock,
        pub effects: Vec<BlockQueryBlockValueExecutedBlockEffects>,
        #[serde(rename = "stateHash")]
        pub state_hash: CryptoHash,
    }
    #[derive(Deserialize)]
    pub struct BlockQueryBlockValueExecutedBlockBlock {
        #[serde(rename = "chainId")]
        pub chain_id: ChainId,
        pub height: BlockHeight,
        pub timestamp: Timestamp,
        #[serde(rename = "authenticatedSigner")]
        pub authenticated_signer: Option<Owner>,
        #[serde(rename = "previousBlockHash")]
        pub previous_block_hash: Option<CryptoHash>,
    }
    #[derive(Deserialize)]
    pub struct BlockQueryBlockValueExecutedBlockEffects {
        pub destination: Destination,
        #[serde(rename = "authenticatedSigner")]
        pub authenticated_signer: Option<Owner>,
    }
}
impl graphql_client::GraphQLQuery for BlockQuery {
    type Variables = block_query::Variables;
    type ResponseData = block_query::ResponseData;
    fn build_query(variables: Self::Variables) -> ::graphql_client::QueryBody<Self::Variables> {
        graphql_client::QueryBody {
            variables,
            query: block_query::QUERY,
            operation_name: block_query::OPERATION_NAME,
        }
    }
}
