<!-- cargo-rdme start -->

# Meta-Counter Example Application

This application demonstrates how `cross_application_call` works in Linera. In our counter example 
which has an `handle_application_call` implemented to handle cross-application calls. To use this application one must publish and create the `counter` application on the local Linera network

# How It Works

It requires the application id of the counter example as a parameter. It sends a message
to itself with a value of `0` to increment so that no value is changed but the parameters and working is 
checked.

To perform the `increment` operation it send a message along with the value to be incremented. 
On receiving the cross-chain message it made a cross-application call to the counter application
whose application id we already provided at the time of creation.

When the counter application recieves a cross application call it increments the value by the specified
amount


# Usage

## Setup 

First, build Linera and add it to the path:

```bash
cargo build
export PATH=$PWD/target/debug:$PATH
```

To start the local Linera network

```bash
linera net up --testing-prng-seed 37
```

This will start the local Linera network. We used the
test-only CLI option `--testing-prng-seed` to make keys deterministic and simplify our
presentation.

Copy and paste `export LINERA_WALLET="/var . . . ."` & `export LINERA_STORAGE="rocksdb:/. . . ."` from output of `linera net up --testing-prng-seed 37` into another terminal

```bash
CHAIN_1=e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
OWNER_1=e814a7bdae091daf4a110ef5340396998e538c47c6e7d101027a225523985316
```

Now, publish-and-create the `counter` application

```bash
(cd examples/counter && cargo build --release)

APPLICATION_ID=$(linera publish-and-create \
  examples/target/wasm32-unknown-unknown/release/counter_{contract,service}.wasm \
  --json-argument "1")
```

Now, compile the `meta-counter` application WebAssembly binaries, publish and create an application instance.

```bash
(cd examples/meta-counter && cargo build --release)
linera publish-and-create \
  examples/target/wasm32-unknown-unknown/release/meta_counter_{contract,service}.wasm \
  --json-argument="null" --json-parameters '"'"$APPLICATION_ID"'"' --required-application-ids $APPLICATION_ID
```

## Using the Counter Application

First, a node service for the current wallet has to be started:

```bash
PORT=8080
linera service --port $PORT &
```

### Using GraphiQL

- Navigate to `http://localhost:8080/chains/$CHAIN_1/applications/$APPLICATION_ID`
- To get the current value of `counter`, run the query
```json
    query{
        value
    }
```
- To increase the value of the counter by 3, perform `increase` operation
```json
    mutation Increment{
        increment(value: 3)
    }
```
- Running the query again would yield `4`

Now, if we check from `counter` application we will also get `4` because eventually we modified the state 

<!-- cargo-rdme end -->
