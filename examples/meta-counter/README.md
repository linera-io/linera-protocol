<!-- cargo-rdme start -->

# Meta-Counter Example Application

This application demonstrates how `cross_application_call` works in Linera.
Our counter example
has an `handle_application_call` implemented to handle cross-application calls. To use this application
one must publish and create the `counter` application on the local Linera network.

# How It Works

It requires the application id of the counter example as a parameter.

To perform the `increment` operation it sends a message along with the value to be added.
On receiving the cross-chain message it makes a cross-application call to the counter application
whose application id we already provided at the time of creation.

When the counter application recieves a cross-application call it increments the value by the specified
amount.

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
CHAIN_1=e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
OWNER_1=e814a7bdae091daf4a110ef5340396998e538c47c6e7d101027a225523985316
```

Now, publish and create the `counter` application. The flag `--wait-for-outgoing-messages` waits until a quorum of validators has confirmed that all sent cross-chain messages have been delivered.

```bash
(cd examples/counter && cargo build --release)

APPLICATION_ID=$(linera --wait-for-outgoing-messages publish-and-create examples/target/wasm32-unknown-unknown/release/counter_{contract,service}.wasm \
  --json-argument "1")
```

Now, compile the `meta-counter` application WebAssembly binaries, publish and create an application instance.

```bash
(cd examples/meta-counter && cargo build --release)
META_COUNTER_ID=$(linera --wait-for-outgoing-messages publish-and-create \
  examples/target/wasm32-unknown-unknown/release/meta_counter_{contract,service}.wasm \
  --json-argument="null" --json-parameters '"'"$APPLICATION_ID"'"' --required-application-ids $APPLICATION_ID)
```

## Using the Meta Counter Application

First, a node service for the current wallet has to be started:

```bash
PORT=8080
linera service --port $PORT &
```

### Using GraphiQL

- Navigate to `http://localhost:8080/chains/$CHAIN_1/applications/$META_COUNTER_ID`.
- To get the current value of `counter`, run the query:
```json
    query {
        value
    }
```
- To increase the value of the counter by 3, perform the `increment` operation:
```json
    mutation Increment {
        increment(value: 3)
    }
```
- Running the query again would yield `4`.

Now, if we check from `counter` application we will also get `4` because eventually we modified the state.

<!-- cargo-rdme end -->
