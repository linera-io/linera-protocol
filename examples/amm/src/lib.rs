// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
# Automated Market Maker (AMM) Example Application

This example implements an Automated Market Maker (AMM) which demonstrates DeFi capabilities of the
Linera protocol. Prerequisite for the AMM application is the `fungible` application, as we will
be adding/removing liquidity and also performing a swap.

# How it works

It supports the following operations. All operations need to be executed remotely.

- Swap: For a given input token and an input amount, it swaps that token amount for an
amount of the other token calculated based on the current AMM ratio.

- Add Liquidity: This operation allows adding liquidity to the AMM. Given a maximum
`token0` and `token1` amount that you're willing to add, it adds liquidity such that you'll be
adding at most `max_token0_amount` of `token0` and `max_token1_amount` of `token1`. The amounts
will be calculated based on the current AMM ratio. The owner, in this case, refers to the user
adding liquidity, which currently can only be a chain owner.

- Remove Liquidity: This withdraws tokens from the AMM. Given the index of the token you'd
like to remove (can be 0 or 1), and an amount of that token that you'd like to remove, it calculates
how much of the other token will also be removed based on the current AMM ratio. Then it removes
the amounts from both tokens as a removal of liquidity. The owner, in this context, is the user
removing liquidity, which currently can only be a chain owner.

# Usage

## Setting Up

Before getting started, make sure that the binary tools `linera*` corresponding to
your version of `linera-sdk` are in your PATH. For scripting purposes, we also assume
that the BASH function `linera_spawn_and_read_wallet_variables` is defined.

From the root of Linera repository, this can be achieved as follows:

```bash
export PATH="$PWD/target/debug:$PATH"
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"
```

To start the local Linera network:

```bash
linera_spawn_and_read_wallet_variables linera net up --testing-prng-seed 37
```

We use the test-only CLI option `--testing-prng-seed` to make keys deterministic and simplify our
explanation.

```bash
OWNER_1=65adbf65c9c1f48a0f1f2d06e0780994dcf8e428ffd5ee53948b3bb6c572c66f
OWNER_2=90d81e6e76ac75497a10a40e689de7b912db61a91b3ae28ed4d908e52e44ef7f
CHAIN_1=dad01517c7a3c428ea903253a9e59964e8db06d323a9bd3f4c74d6366832bdbf
CHAIN_2=e54bdb17d41d5dbe16418f96b70e44546ccd63e6f3733ae3c192043548998ff3
CHAIN_AMM=e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
OWNER_AMM=7136460f0c87ae46f966f898d494c4b40c4ae8c527f4d1c0b1fa0f7cff91d20f
```

Now we have to publish and create the fungible applications. The flag `--wait-for-outgoing-messages` waits until a quorum of validators has confirmed that all sent cross-chain messages have been delivered.

```bash
(cd examples/fungible && cargo build --release --target wasm32-unknown-unknown)

FUN1_APP_ID=$(linera --wait-for-outgoing-messages \
  publish-and-create examples/target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm \
    --json-argument "{ \"accounts\": {
        \"User:$OWNER_AMM\": \"100.\"
    } }" \
    --json-parameters "{ \"ticker_symbol\": \"FUN1\" }" \
)

FUN2_APP_ID=$(linera --wait-for-outgoing-messages \
  publish-and-create examples/target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm \
    --json-argument "{ \"accounts\": {
        \"User:$OWNER_AMM\": \"100.\"
    } }" \
    --json-parameters "{ \"ticker_symbol\": \"FUN2\" }" \
)

(cd examples/amm && cargo build --release --target wasm32-unknown-unknown)
AMM_APPLICATION_ID=$(linera --wait-for-outgoing-messages \
  publish-and-create examples/target/wasm32-unknown-unknown/release/amm_{contract,service}.wasm \
  --json-parameters "{\"tokens\":["\"$FUN1_APP_ID\"","\"$FUN2_APP_ID\""]}" \
  --required-application-ids $FUN1_APP_ID $FUN2_APP_ID)
```

## Using the AMM Application

First, a node service for the current wallet has to be started:

```bash
PORT=8080
linera service --port $PORT &
```

### Using GraphiQL

To properly setup the tokens in the proper chains, we need to do some transfer operations:

- Transfer 50 FUN1 from `$OWNER_AMM` in `$CHAIN_AMM` to `$OWNER_1` in `$CHAIN_1`, so they're in the proper chain
```gql,uri=http://localhost:8080/chains/$CHAIN_AMM/applications/$FUN1_APP_ID
    mutation {
        transfer(
            owner: "User:7136460f0c87ae46f966f898d494c4b40c4ae8c527f4d1c0b1fa0f7cff91d20f",
            amount: "50.",
            targetAccount: {
                chainId: "dad01517c7a3c428ea903253a9e59964e8db06d323a9bd3f4c74d6366832bdbf",
                owner: "User:65adbf65c9c1f48a0f1f2d06e0780994dcf8e428ffd5ee53948b3bb6c572c66f",
            }
        )
    }
```

- Transfer 50 FUN1 from `$OWNER_AMM` in `$CHAIN_AMM` to `$OWNER_2` in `$CHAIN_2`, so they're in the proper chain
```gql,uri=http://localhost:8080/chains/$CHAIN_AMM/applications/$FUN1_APP_ID
    mutation {
        transfer(
            owner: "User:7136460f0c87ae46f966f898d494c4b40c4ae8c527f4d1c0b1fa0f7cff91d20f",
            amount: "50.",
            targetAccount: {
                chainId: "e54bdb17d41d5dbe16418f96b70e44546ccd63e6f3733ae3c192043548998ff3",
                owner: "User:90d81e6e76ac75497a10a40e689de7b912db61a91b3ae28ed4d908e52e44ef7f",
            }
        )
    }
```

- Transfer 50 FUN2 from `$OWNER_AMM` in `$CHAIN_AMM` to `$OWNER_1` in `$CHAIN_1`, so they're in the proper chain
```gql,uri=http://localhost:8080/chains/$CHAIN_AMM/applications/$FUN2_APP_ID
    mutation {
        transfer(
            owner: "User:7136460f0c87ae46f966f898d494c4b40c4ae8c527f4d1c0b1fa0f7cff91d20f",
            amount: "50.",
            targetAccount: {
                chainId: "dad01517c7a3c428ea903253a9e59964e8db06d323a9bd3f4c74d6366832bdbf",
                owner: "User:65adbf65c9c1f48a0f1f2d06e0780994dcf8e428ffd5ee53948b3bb6c572c66f",
            }
        )
    }
```

- Transfer 50 FUN2 from `$OWNER_AMM` in `$CHAIN_AMM` to `$OWNER_2` in `$CHAIN_2`, so they're in the proper chain
```gql,uri=http://localhost:8080/chains/$CHAIN_AMM/applications/$FUN2_APP_ID
    mutation {
        transfer(
            owner: "User:7136460f0c87ae46f966f898d494c4b40c4ae8c527f4d1c0b1fa0f7cff91d20f",
            amount: "50.",
            targetAccount: {
                chainId: "e54bdb17d41d5dbe16418f96b70e44546ccd63e6f3733ae3c192043548998ff3",
                owner: "User:90d81e6e76ac75497a10a40e689de7b912db61a91b3ae28ed4d908e52e44ef7f",
            }
        )
    }
```

All operations can only be from a remote chain i.e. other than the chain on which `AMM` is deployed to.
We can do it from GraphiQL by performing the `requestApplication` mutation so that we can perform the
operation from the chain.

```gql,uri=http://localhost:8080
mutation {
  requestApplication (
    chainId:"dad01517c7a3c428ea903253a9e59964e8db06d323a9bd3f4c74d6366832bdbf",
    applicationId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65060000000000000000000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65080000000000000000000000",
    targetChainId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
  )
}
```
Note: The above mutation has to be performed from `http://localhost:8080`.

Before performing any operation we need to provide liquidity to it, so we will use the `AddLiquidity` operation,
navigate to `http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID`.

To perform `AddLiquidity` operation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID
mutation {
  addLiquidity(
    owner: "User:65adbf65c9c1f48a0f1f2d06e0780994dcf8e428ffd5ee53948b3bb6c572c66f",
    maxToken0Amount: "50",
    maxToken1Amount: "50",
  )
}
```

```gql,uri=http://localhost:8080
mutation {
  requestApplication (
    chainId:"e54bdb17d41d5dbe16418f96b70e44546ccd63e6f3733ae3c192043548998ff3",
    applicationId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65060000000000000000000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65080000000000000000000000",
    targetChainId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
  )
}
```
Note: The above mutation has to be performed from `http://localhost:8080`.

To perform `Swap` operation, navigate to `http://localhost:8080/chains/$CHAIN_2/applications/$AMM_APPLICATION_ID` and
perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_2/applications/$AMM_APPLICATION_ID
mutation {
  swap(
    owner: "User:90d81e6e76ac75497a10a40e689de7b912db61a91b3ae28ed4d908e52e44ef7f",
    inputTokenIdx: 1,
    inputAmount: "1",
  )
}
```

To perform the `RemoveLiquidity` operation, navigate to `http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID` and
perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID
mutation {
  removeLiquidity(
    owner: "User:65adbf65c9c1f48a0f1f2d06e0780994dcf8e428ffd5ee53948b3bb6c572c66f",
    tokenToRemoveIdx: 1,
    tokenToRemoveAmount: "1",
  )
}
```

To perform the `RemoveAllAddedLiquidity` operation, navigate to `http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID` and
perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID
mutation {
  removeAllAddedLiquidity(
    owner: "User:65adbf65c9c1f48a0f1f2d06e0780994dcf8e428ffd5ee53948b3bb6c572c66f",
  )
}
```

### Atomic Swaps

In general, if you send tokens to a chain owned by someone else, you rely on them
for asset availability: If they don't handle your messages, you don't have access to
your tokens.

Fortunately, Linera provides a solution based on temporary chains:
If the number of parties who want to swap tokens is limited, we can make them all chain
owners, allow only AMM operations on the chain, and allow only the AMM to close the chain.

```bash
PUB_KEY_AMM=fcf518d56455283ace2bbc11c71e684eb58af81bc98b96a18129e825ce24ea84
PUB_KEY_2=ca909dcf60df014c166be17eb4a9f6e2f9383314a57510206a54cd841ade455e

kill %% && sleep 1    # Kill the service so we can use CLI commands for chain 1.

linera --wait-for-outgoing-messages change-ownership \
    --owner-public-keys $PUB_KEY_AMM $PUB_KEY_2

linera --wait-for-outgoing-messages change-application-permissions \
    --execute-operations $AMM_APPLICATION_ID \
    --close-chain $AMM_APPLICATION_ID

linera service --port $PORT &
```

First, let's add some liquidity again to the AMM. Navigate to
`http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID` and perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID
mutation {
  addLiquidity(
    owner: "User:65adbf65c9c1f48a0f1f2d06e0780994dcf8e428ffd5ee53948b3bb6c572c66f",
    maxToken0Amount: "40",
    maxToken1Amount: "40",
  )
}
```

The only way to close the chain is via the application. Navigate to
`http://localhost:8080/chains/$CHAIN_AMM/applications/$AMM_APPLICATION_ID` and perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_AMM/applications/$AMM_APPLICATION_ID
mutation { closeChain }
```

Owner 1 should now get back their tokens, and have around 49 FUN1 left and 51 FUN2 left. To check that, navigate
to `http://localhost:8080/chains/$CHAIN_1/applications/$FUN1_APP_ID`, and perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$FUN1_APP_ID
query {
    accounts {
        entry(
            key: "User:65adbf65c9c1f48a0f1f2d06e0780994dcf8e428ffd5ee53948b3bb6c572c66f"
        ) {
            value
        }
    }
}
```

Then navigate to `http://localhost:8080/chains/$CHAIN_1/applications/$FUN2_APP_ID`, and perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$FUN2_APP_ID
query {
    accounts {
        entry(
            key: "User:65adbf65c9c1f48a0f1f2d06e0780994dcf8e428ffd5ee53948b3bb6c572c66f"
        ) {
            value
        }
    }
}
```
*/

use async_graphql::{scalar, Request, Response};
use linera_sdk::{
    base::{AccountOwner, Amount, ContractAbi, ServiceAbi},
    graphql::GraphQLMutationRoot,
};
pub use matching_engine::Parameters;
use serde::{Deserialize, Serialize};

pub struct AmmAbi;

impl ContractAbi for AmmAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for AmmAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// Operations that can be sent to the application.
#[derive(Debug, Serialize, Deserialize, GraphQLMutationRoot)]
pub enum Operation {
    // TODO(#969): Need to also implement Swap Bids here
    /// Swap operation
    /// Given an input token idx (can be 0 or 1), and an input amount,
    /// Swap that token amount for an amount of the other token,
    /// calculated based on the current AMM ratio
    /// Owner here is the user executing the Swap
    Swap {
        owner: AccountOwner,
        input_token_idx: u32,
        input_amount: Amount,
    },
    /// Add liquidity operation
    /// Given a maximum token0 and token1 amount that you're willing to add,
    /// add liquidity to the AMM such that you'll be adding AT MOST
    /// `max_token0_amount` of token0, and `max_token1_amount` of token1,
    /// which will be calculated based on the current AMM ratio
    /// Owner here is the user adding liquidity, which currently can only
    /// be a chain owner
    AddLiquidity {
        owner: AccountOwner,
        max_token0_amount: Amount,
        max_token1_amount: Amount,
    },
    /// Remove liquidity operation
    /// Given a token idx of the token you'd like to remove (can be 0 or 1),
    /// and an amount of that token that you'd like to remove, we'll calculate
    /// how much of the other token will also be removed, which will be calculated
    /// based on the current AMM ratio. Then remove the amounts from both tokens
    /// as a removal of liquidity
    /// Owner here is the user removing liquidity, which currently can only
    /// be a chain owner
    RemoveLiquidity {
        owner: AccountOwner,
        token_to_remove_idx: u32,
        token_to_remove_amount: Amount,
    },
    /// Remove all added liquidity operation
    /// Remove all the liquidity added by given user, that is remaining in the AMM.
    /// Owner here is the user removing liquidity, which currently can only
    /// be a chain owner
    RemoveAllAddedLiquidity { owner: AccountOwner },
    /// Close this chain, and remove all added liquidity
    /// Requires that this application is authorized to close the chain.
    CloseChain,
}

scalar!(Operation);

#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    Swap {
        owner: AccountOwner,
        input_token_idx: u32,
        input_amount: Amount,
    },
    AddLiquidity {
        owner: AccountOwner,
        max_token0_amount: Amount,
        max_token1_amount: Amount,
    },
    RemoveLiquidity {
        owner: AccountOwner,
        token_to_remove_idx: u32,
        token_to_remove_amount: Amount,
    },
    RemoveAllAddedLiquidity {
        owner: AccountOwner,
    },
}
