#![allow(clippy::all, warnings)]
pub struct BlocksQuery;
pub mod blocks_query {
    #![allow(dead_code)]
    use std::result::Result;
    pub const OPERATION_NAME: &str = "BlocksQuery";
    pub const QUERY : & str = "query BlocksQuery($from: CryptoHash, $chainId: ChainId, $limit: Int) {\n  blocks(from: $from, chainId: $chainId, limit: $limit) {\n    hash\n    value {\n      status\n      round\n      executedBlock {\n        block {\n          chainId\n          # epoch\n          height\n          timestamp\n          authenticatedSigner\n          previousBlockHash\n          # incomingMessages {\n          #   origin\n          #   event\n          # }\n          # operations\n        }\n        effects {\n          destination\n          authenticatedSigner\n          # effect\n        }\n        stateHash\n      }\n    }\n  }\n}\n" ;
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
        pub from: Option<CryptoHash>,
        #[serde(rename = "chainId")]
        pub chain_id: Option<ChainId>,
        pub limit: Option<Int>,
    }
    impl Variables {}
    #[derive(Deserialize)]
    pub struct ResponseData {
        pub blocks: Vec<BlocksQueryBlocks>,
    }
    #[derive(Deserialize)]
    pub struct BlocksQueryBlocks {
        pub hash: CryptoHash,
        pub value: BlocksQueryBlocksValue,
    }
    #[derive(Deserialize)]
    pub struct BlocksQueryBlocksValue {
        pub status: String,
        pub round: RoundNumber,
        #[serde(rename = "executedBlock")]
        pub executed_block: BlocksQueryBlocksValueExecutedBlock,
    }
    #[derive(Deserialize)]
    pub struct BlocksQueryBlocksValueExecutedBlock {
        pub block: BlocksQueryBlocksValueExecutedBlockBlock,
        pub effects: Vec<BlocksQueryBlocksValueExecutedBlockEffects>,
        #[serde(rename = "stateHash")]
        pub state_hash: CryptoHash,
    }
    #[derive(Deserialize)]
    pub struct BlocksQueryBlocksValueExecutedBlockBlock {
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
    pub struct BlocksQueryBlocksValueExecutedBlockEffects {
        pub destination: Destination,
        #[serde(rename = "authenticatedSigner")]
        pub authenticated_signer: Option<Owner>,
    }
}
impl graphql_client::GraphQLQuery for BlocksQuery {
    type Variables = blocks_query::Variables;
    type ResponseData = blocks_query::ResponseData;
    fn build_query(variables: Self::Variables) -> ::graphql_client::QueryBody<Self::Variables> {
        graphql_client::QueryBody {
            variables,
            query: blocks_query::QUERY,
            operation_name: blocks_query::OPERATION_NAME,
        }
    }
}
