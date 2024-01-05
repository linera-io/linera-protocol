<!-- cargo-rdme start -->

# Matching Engine Example Application

This sample application demonstrates a matching engine, showcasing the DeFi capabilities
on the Linera protocol.

The matching engine trades between two tokens `token_0` & `token_1`. We can refer to
the `fungible` application example on how to create two token applications.

An order can be of two types:

- Bid: For buying token 1 and paying in token 0, these are ordered in from the highest
  bid (most preferable) to the lowest price.
- Ask: For selling token 1, to be paid in token 0, these are ordered from the lowest
  (most preferable) to the highest price.

An `OrderId` is used to uniquely identify an order and enables the following functionality:

- Modify: Allows to modify the order.
- Cancel: Cancelling an order.

When inserting an order it goes through the following steps:

- Transfer of tokens from the `fungible` application to the `matching engine` application through a cross-application
call so that it can be paid to the counterparty.

- The engine selects the matching price levels for the inserted order. It then proceeds
  to clear these levels, executing trades and ensuring that at the end of the process,
  the best bid has a higher price than the best ask. This involves adjusting the orders in the market
  and potentially creating a series of transfer orders for the required tokens. If, after
  the level clearing, the order is completely filled, it is not inserted. Otherwise,
  it becomes a liquidity order in the matching engine, providing depth to the market
  and potentially being matched with future orders.

When an order is created from a remote chain, it transfers the tokens of the same owner
from the remote chain to the chain of the matching engine, and a `ExecuteOrder` message is sent with the order details.

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

Publish and create two `fungible` application whose `application_id` will be used as a
parameter while creating the `matching engine` example.

```bash
(cd examples/fungible && cargo build --release)

FUN1_APP_ID=$(linera publish-and-create examples/target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm \
    --json-argument "{ \"accounts\": {
        \"User:$OWNER_1\": \"100.\",
        \"User:$OWNER_2\": \"150.\"
    } }" \
    --json-parameters "{ \"ticker_symbol\": \"FUN1\" }" \
)

FUN2_APP_ID=$(linera publish-and-create examples/target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm \
    --json-argument "{ \"accounts\": {
        \"User:$OWNER_1\": \"100.\",
        \"User:$OWNER_2\": \"150.\"
    } }" \
    --json-parameters "{ \"ticker_symbol\": \"FUN2\" }" \
)

```

Now we have to publish and deploy the Matching Engine application:

```bash
(cd examples/matching-engine && cargo build --release)
MATCHING_ENGINE=$(linera publish-and-create examples/target/wasm32-unknown-unknown/release/matching_engine_{contract,service}.wasm --json-parameters "{\"tokens\":["\"$FUN1_APP_ID\"","\"$FUN1_APP_ID\""]}")
```

## Using the Matching Engine Application

First, a node service for the current wallet has to be started:

```bash
PORT=8080
linera service --port $PORT &
```

### Using GraphiQL

Navigate to `http://localhost:8080/chains/$CHAIN_1/applications/$MATCHING_ENGINE`.

To create a `Bid` order nature:

```json
mutation ExecuteOrder {
  executeOrder(
    order:{
        Insert : {
        owner: "User:e814a7bdae091daf4a110ef5340396998e538c47c6e7d101027a225523985316",
        amount: "1",
        nature: Bid,
        price: {
        	price:5
        }
      }
  	}
  )
}
```

To query about the bid price:

```json
query{
  bids {
    keys{
      price
    }
  }
}
```

<!-- cargo-rdme end -->
