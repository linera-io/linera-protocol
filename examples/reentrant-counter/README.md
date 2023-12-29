<!-- cargo-rdme start -->

# Reentrant-Counter Example Application

This application just works like `counter` application i.e. increaments the value, but internal working 
is different than that of the `counter` application

# How It Works

It does increaments the value but instead of increamenting the value in a single operation 
it instead divides it into two halves, one half is increamented via `operation` and the other 
half via `application_call`, hence making a re-entry.

For example, let the current value be 1, now we want to increase its value by `4`. The four would be 
divided into halves. `first_half` = 2 and `second_half` = 2, the first half 
 
# Usage

## Setup 

First, build Linera and add it to the path:

```bash
cargo build
export PATH=$PWD/target/debug:$PATH
```

To start the local linera network

```bash
linera net up --testing-prng-seed 37
```

This will start the local linera network. We used the
test-only CLI option `--testing-prng-seed` to make keys deterministic and simplify our
presentation.

Copy and paste `export LINERA_WALLET="/var . . . ."` & `export LINERA_STORAGE="rocksdb:/. . . ."` from output of `linera net up --testing-prng-seed 37` into another terminal

```bash
CHAIN_1=e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
OWNER_1=e814a7bdae091daf4a110ef5340396998e538c47c6e7d101027a225523985316
```

Now, compile the `reentrant-counter` application WebAssembly binaries, publish and create an application instance.

```bash
(cd examples/reentrant-counter && cargo build --release)
APPLICATION_ID=$(linera publish-and-create \
  ../target/wasm32-unknown-unknown/release/reentrant_counter_{contract,service}.wasm \
  --json-argument "1")
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
- To increase the value of the counter by 4, perform `increase` operation
```json
    mutation Increament{
        increment(value: 4)
    }
```
- Running the query again would yield `5`

<!-- cargo-rdme end -->
