<!-- cargo-rdme start -->

# Automated Market Maker (AMM) Example Application

This example implements an Automated Market Maker (AMM) which demonstrates DeFi capabilities of the
Linera protocol. Prerequisite for the AMM application is the `fungible` application, as we will
be adding/removing liquidity and also performing a swap.

# How it works

It supports the following operations.

- Swap: For a given input token and an input amount, it swaps that token amount for an
amount of the other token calculated based on the current AMM ratio. Note: The `Swap` operations
need to be performed from a remote chain.

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
OWNER_1=e814a7bdae091daf4a110ef5340396998e538c47c6e7d101027a225523985316
OWNER_2=453690095cdfe6dbde7fc577e56bb838a7ee7920a72512d4a87748b4e151ed61
CHAIN_1=e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
CHAIN_2=e54bdb17d41d5dbe16418f96b70e44546ccd63e6f3733ae3c192043548998ff3
```

Now we have to publish and create the fungible applications. The flag `--wait-for-outgoing-messages` waits until a quorum of validators has confirmed that all sent cross-chain messages have been delivered.

```bash
(cd examples/fungible && cargo build --release)

FUN1_APP_ID=$(linera --wait-for-outgoing-messages \
  publish-and-create examples/target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm \
    --json-argument "{ \"accounts\": {
        \"User:$OWNER_1\": \"100.\",
        \"User:$OWNER_2\": \"150.\"
    } }" \
    --json-parameters "{ \"ticker_symbol\": \"FUN1\" }" \
)

FUN2_APP_ID=$(linera --wait-for-outgoing-messages \
  publish-and-create examples/target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm \
    --json-argument "{ \"accounts\": {
        \"User:$OWNER_1\": \"100.\",
        \"User:$OWNER_2\": \"150.\"
    } }" \
    --json-parameters "{ \"ticker_symbol\": \"FUN2\" }" \
)

(cd examples/amm && cargo build --release)
AMM_APPLICATION_ID=$(linera --wait-for-outgoing-messages publish-and-create examples/target/wasm32-unknown-unknown/release/amm_{contract,service}.wasm --json-parameters "{\"tokens\":["\"$FUN1_APP_ID\"","\"$FUN2_APP_ID\""]}")
```

## Using the AMM Application

First, a node service for the current wallet has to be started:

```bash
PORT=8080
linera service --port $PORT &
```

### Using GraphiQL

Before performing any operation we need to provide liquidity to it, so we will use the `AddLiquidity` operation,
navigate to `http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID`.

To perform `AddLiquidity` operation:

```json
mutation{
  operation(
    operation: {
      AddLiquidity: {
        owner:"User:e814a7bdae091daf4a110ef5340396998e538c47c6e7d101027a225523985316",
        max_token0_amount: "50",
        max_token1_amount: "40",
      }
    }
  )
}
```

We can only perform `Swap` from a remote chain i.e. other than the chain on which `AMM` is deployed to,
we can do it from GraphiQL by performing the `requestApplication` mutation so that we can perform the
`Swap` operation from the chain.

```json
mutation {
  requestApplication (
    chainId:"e54bdb17d41d5dbe16418f96b70e44546ccd63e6f3733ae3c192043548998ff3",
    applicationId: "AMM_APPLICATION_ID",
    targetChainId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
  )
}
```
Note: The above mutation has to be performed from `http://localhost:8080`.

Now to perform `Swap` operation, naviage to `http://localhost:8080/chains/$CHAIN_2/applications/$AMM_APPLICATION_ID` and
perform the following mutation:

```json
mutation{
  operation(
    operation: {
      Swap: {
        owner:"User:453690095cdfe6dbde7fc577e56bb838a7ee7920a72512d4a87748b4e151ed61",
        input_token_idx: 1,
        input_amount: "1",
      }
    }
  )
}
```

We can also perform the `RemoveLiquidity` operation, navigate to `http://localhost:8080/chains/$CHAIN_1/applications/$AMM_APPLICATION_ID` and
perform the following mutation:

```json
mutation{
  operation(
    operation: {
      RemoveLiquidity: {
        owner:"User:453690095cdfe6dbde7fc577e56bb838a7ee7920a72512d4a87748b4e151ed61",
        token_to_remove_idx: 1,
        token_to_remove_amount: "1",
      }
    }
  )
}
```

<!-- cargo-rdme end -->
