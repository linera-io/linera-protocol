# Automated Market Maker (AMM) Example Application

This example implements an Automated Market Maker (AMM) which demonstrates DeFi capabilities of the
Linera protocol. Prerequisite for the AMM application is the `fungible` application, as we will
be adding/removing liquidity and also performing a swap.

## How it works

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

## Usage

### Setting Up

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
OWNER_1=d2115775b5b3c5c1ed3c1516319a7e850c75d0786a74b39f5250cf9decc88124
OWNER_2=a477cb966190661c0dfbe50602616a78a48d2bef6cb5288d49deb3e05585d579
CHAIN_1=673ce04da4b8ed773ee7cd5828a2083775bea4130498b847c5b34b2ed913b07f
CHAIN_2=69705f85ac4c9fef6c02b4d83426aaaf05154c645ec1c61665f8e450f0468bc0
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

### Using the AMM Application

First, a node service for the current wallet has to be started:

```bash
PORT=8080
linera service --port $PORT &
```

#### Using GraphiQL

Type each of these in the GraphiQL interface and substitute the env variables with their actual
values that we've defined above.

To properly setup the tokens in the proper chains, we need to do some transfer operations:

- Transfer 50 FUN1 from `$OWNER_AMM` in `$CHAIN_AMM` to `$OWNER_1` in `$CHAIN_1`, so they're in the proper chain.
  Run `echo "http://localhost:8080/chains/$CHAIN_AMM/applications/$FUN1_APP_ID"` to print the URL
  of the GraphiQL interface for the FUN1 app. Navigate to that URL and enter:

```gql,uri=http://localhost:8080/chains/$CHAIN_AMM/applications/$FUN1_APP_ID
mutation {
  transfer(
    owner: "User:$OWNER_AMM",
    amount: "50.",
    targetAccount: {
      chainId: "$CHAIN_1",
      owner: "User:$OWNER_1",
    }
  )
}
```

- Transfer 50 FUN1 from `$OWNER_AMM` in `$CHAIN_AMM` to `$OWNER_2` in `$CHAIN_2`, so they're in the proper chain:

```gql,uri=http://localhost:8080/chains/$CHAIN_AMM/applications/$FUN1_APP_ID
mutation {
  transfer(
    owner: "User:$OWNER_AMM",
    amount: "50.",
    targetAccount: {
      chainId: "$CHAIN_2",
      owner: "User:$OWNER_2",
    }
  )
}
```

- Transfer 50 FUN2 from `$OWNER_AMM` in `$CHAIN_AMM` to `$OWNER_1` in `$CHAIN_1`, so they're in the proper chain.
  Since this is the other token, FUN2, we need to go to its own GraphiQL interface:
  `echo "http://localhost:8080/chains/$CHAIN_AMM/applications/$FUN2_APP_ID"`.

```gql,uri=http://localhost:8080/chains/$CHAIN_AMM/applications/$FUN2_APP_ID
mutation {
  transfer(
    owner: "User:$OWNER_AMM",
    amount: "50.",
    targetAccount: {
      chainId: "$CHAIN_1",
      owner: "User:$OWNER_1",
    }
  )
}
```

- Transfer 50 FUN2 from `$OWNER_AMM` in `$CHAIN_AMM` to `$OWNER_2` in `$CHAIN_2`, so they're in the proper chain:

```gql,uri=http://localhost:8080/chains/$CHAIN_AMM/applications/$FUN2_APP_ID
mutation {
  transfer(
    owner: "User:$OWNER_AMM",
    amount: "50.",
    targetAccount: {
      chainId: "$CHAIN_2",
      owner: "User:$OWNER_2",
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
    chainId:"$CHAIN_1",
    applicationId: "$AMM_APPLICATION_ID",
    targetChainId: "$CHAIN_AMM"
  )
}
```

Note: The above mutation has to be performed from `http://localhost:8080`.

Before performing any operation we need to provide liquidity to it, so we will use the `AddLiquidity` operation,
navigate to the URL you get by running `echo "http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID"`.

To perform the `AddLiquidity` operation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID
mutation {
  addLiquidity(
    owner: "User:$OWNER_1",
    maxToken0Amount: "50",
    maxToken1Amount: "50",
  )
}
```

```gql,uri=http://localhost:8080
mutation {
  requestApplication (
    chainId:"$CHAIN_2",
    applicationId: "$AMM_APPLICATION_ID",
    targetChainId: "$CHAIN_AMM"
  )
}
```

Note: The above mutation has to be performed from `http://localhost:8080`.

To perform the `Swap` operation, navigate to the URL you get by running `echo "http://localhost:8080/chains/$CHAIN_2/applications/$AMM_APPLICATION_ID"` and
perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_2/applications/$AMM_APPLICATION_ID
mutation {
  swap(
    owner: "User:$OWNER_2",
    inputTokenIdx: 1,
    inputAmount: "1",
  )
}
```

To perform the `RemoveLiquidity` operation, navigate to the URL you get by running `echo "http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID"` and
perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID
mutation {
  removeLiquidity(
    owner: "User:$OWNER_1",
    tokenToRemoveIdx: 1,
    tokenToRemoveAmount: "1",
  )
}
```

To perform the `RemoveAllAddedLiquidity` operation, navigate to the URL you get by running `echo "http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID"` and
perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID
mutation {
  removeAllAddedLiquidity(
    owner: "User:$OWNER_1",
  )
}
```

#### Atomic Swaps

In general, if you send tokens to a chain owned by someone else, you rely on them
for asset availability: If they don't handle your messages, you don't have access to
your tokens.

Fortunately, Linera provides a solution based on temporary chains:
If the number of parties who want to swap tokens is limited, we can make them all chain
owners, allow only AMM operations on the chain, and allow only the AMM to close the chain.
In addition, we make an AMM operation per block mandatory, so owners cannot spam the chain
with empty blocks.

```bash
kill %% && sleep 1    # Kill the service so we can use CLI commands for chain 1.

linera --wait-for-outgoing-messages change-ownership \
    --owners $OWNER_AMM $OWNER_2

linera --wait-for-outgoing-messages change-application-permissions \
    --execute-operations $AMM_APPLICATION_ID \
    --mandatory-applications $AMM_APPLICATION_ID \
    --close-chain $AMM_APPLICATION_ID

linera service --port $PORT &
```

First, let's add some liquidity again to the AMM. Navigate to the URL you get by running
`echo "http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID"` and perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID
mutation {
  addLiquidity(
    owner: "User:$OWNER_1",
    maxToken0Amount: "40",
    maxToken1Amount: "40",
  )
}
```

The only way to close the chain is via the application. Navigate to the URL you get by running
`echo "http://localhost:8080/chains/$CHAIN_AMM/applications/$AMM_APPLICATION_ID"` and perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_AMM/applications/$AMM_APPLICATION_ID
mutation { closeChain }
```

Owner 1 should now get back their tokens, and have around 49 FUN1 left and 51 FUN2 left. To check that, navigate
to the URL you get by running `echo "http://localhost:8080/chains/$CHAIN_1/applications/$FUN1_APP_ID"`, and perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$FUN1_APP_ID
query {
    accounts {
        entry(
            key: "User:$OWNER_1"
        ) {
            value
        }
    }
}
```

Then navigate to the URL you get by running `echo "http://localhost:8080/chains/$CHAIN_1/applications/$FUN2_APP_ID"`, and perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$FUN2_APP_ID
query {
    accounts {
        entry(
            key: "User:$OWNER_1"
        ) {
            value
        }
    }
}
```
