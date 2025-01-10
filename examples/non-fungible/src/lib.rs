// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Non-Fungible Token Example Application */

use std::fmt::{Display, Formatter};

use async_graphql::{InputObject, Request, Response, SimpleObject};
use fungible::Account;
use linera_sdk::{
    base::{AccountOwner, ApplicationId, ChainId, ContractAbi, ServiceAbi},
    graphql::GraphQLMutationRoot,
    DataBlobHash, ToBcsBytes,
};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Ord, PartialOrd, SimpleObject, InputObject,
)]
#[graphql(input_name = "TokenIdInput")]
pub struct TokenId {
    pub id: Vec<u8>,
}

pub struct NonFungibleTokenAbi;

impl ContractAbi for NonFungibleTokenAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for NonFungibleTokenAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// An operation.
#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum Operation {
    /// Mints a token
    Mint {
        minter: AccountOwner,
        name: String,
        blob_hash: DataBlobHash,
    },
    /// Transfers a token from a (locally owned) account to a (possibly remote) account.
    Transfer {
        source_owner: AccountOwner,
        token_id: TokenId,
        target_account: Account,
    },
    /// Same as `Transfer` but the source account may be remote. Depending on its
    /// configuration, the target chain may take time or refuse to process
    /// the message.
    Claim {
        source_account: Account,
        token_id: TokenId,
        target_account: Account,
    },
}

/// A message.
#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    /// Transfers to the given `target` account, unless the message is bouncing, in which case
    /// we transfer back to the `source`.
    Transfer { nft: Nft, target_account: Account },

    /// Claims from the given account and starts a transfer to the target account.
    Claim {
        source_account: Account,
        token_id: TokenId,
        target_account: Account,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone, SimpleObject, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Nft {
    pub token_id: TokenId,
    pub owner: AccountOwner,
    pub name: String,
    pub minter: AccountOwner,
    pub blob_hash: DataBlobHash,
}

#[derive(Debug, Serialize, Deserialize, Clone, SimpleObject, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NftOutput {
    pub token_id: String,
    pub owner: AccountOwner,
    pub name: String,
    pub minter: AccountOwner,
    pub payload: Vec<u8>,
}

impl NftOutput {
    pub fn new(nft: Nft, payload: Vec<u8>) -> Self {
        use base64::engine::{general_purpose::STANDARD_NO_PAD, Engine as _};
        let token_id = STANDARD_NO_PAD.encode(nft.token_id.id);
        Self {
            token_id,
            owner: nft.owner,
            name: nft.name,
            minter: nft.minter,
            payload,
        }
    }

    pub fn new_with_token_id(token_id: String, nft: Nft, payload: Vec<u8>) -> Self {
        Self {
            token_id,
            owner: nft.owner,
            name: nft.name,
            minter: nft.minter,
            payload,
        }
    }
}

impl Display for TokenId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.id)
    }
}

impl Nft {
    pub fn create_token_id(
        chain_id: &ChainId,
        application_id: &ApplicationId,
        name: &String,
        minter: &AccountOwner,
        blob_hash: &DataBlobHash,
        num_minted_nfts: u64,
    ) -> Result<TokenId, bcs::Error> {
        use sha3::Digest as _;

        let mut hasher = sha3::Sha3_256::new();
        hasher.update(chain_id.to_bcs_bytes()?);
        hasher.update(application_id.to_bcs_bytes()?);
        hasher.update(name);
        hasher.update(name.len().to_bcs_bytes()?);
        hasher.update(minter.to_bcs_bytes()?);
        hasher.update(blob_hash.to_bcs_bytes()?);
        hasher.update(num_minted_nfts.to_bcs_bytes()?);

        Ok(TokenId {
            id: hasher.finalize().to_vec(),
        })
    }
}
