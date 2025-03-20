# How to perform HTTP requests

This example application demonstrates how to perform HTTP requests from the service and from the
contract, in a few different ways:

- From the service while handling a mutation.
- From the contract directly.
- From the service when it is being used as an oracle by the contract.

## HTTP requests from the service

The service is executed either on the client when requested by the user or on validators when the
service is queried as an oracle by the contract. In this first usage scenario, the HTTP request is
executed only in the client.

The HTTP response can then be used by the service to either prepare a query response to the caller
or to prepare operations to be executed by the contract in a block proposal.

## HTTP requests from the contract

The contract can perform HTTP requests as well, but the responses must always be the same. The
requests are executed on the client and on all the validators. That means that the client and each
validator perform the HTTP request independently. The responses must all match (or at least match
in a quorum of validators) for the block the be confirmed.

If the response varies per request (as a simple example, due to the presence of a "Date" timestamp
header in the response), the block proposal may end up being rejected by the validators. If there's
a risk of that happening, the contract should instead call the service as an oracle, and let the
service perform the HTTP request and return only the deterministic parts of the response.

## HTTP requests using the service as an oracle

The contract may call the service as an oracle. That means that that contracts sends a query to the
service and waits for its response. The execution of the contract is metered by executed
instruction, while the service executing as an oracle is metered by a coarse-grained timer. That
allows the service to execute non-deterministically, and as long as it always returns a
deterministic response back to the contract, the validators will agree on its execution and reach
consensus.

In this scenario, the contract requests the service to perform the HTTP request. The HTTP request
is also executed in each validator.

## Recommendation

It is recommended to minimize the number of HTTP requests performed in total, in order to reduce
costs. Whenever possible, it's best to perform the request in the client using the service, and
forward only the HTTP response to the contract. The contract should then verify that the response
can be trusted.

If there's no way to verify an off-chain HTTP response in the contract, then the request should be
made in the contract. However, if there's a risk of receiving different HTTP responses among the
validators, the contract should use the service as oracle to perform the HTTP request and return to
the contract only the data that is deterministic. Using the service as an oracle is more expensive,
so it should be avoided if possible.

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
CHAIN_1=aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8
OWNER_1=de166237331a2966d8cf6778e81a8c007b4084be80dc1e0409d51f216c1deaa1
```

Now, compile the application WebAssembly binaries, publish and create an application instance.

```bash
(cd examples/how-to/perform-http-requests && cargo build --release --target wasm32-unknown-unknown)

APPLICATION_ID=$(linera publish-and-create \
  examples/target/wasm32-unknown-unknown/release/how_to_perform_http_requests_{contract,service}.wasm)
```

The `APPLICATION_ID` is saved so that it can be used in the GraphQL URL later. But first the
service that handles the GraphQL requests must be started.

```bash
PORT=8080
linera service --port $PORT &
```

#### Using GraphiQL

Type each of these in the GraphiQL interface and substitute the env variables with their actual
values that we've defined above.

- Navigate to the URL you get by running `echo "http://localhost:8080/chains/$CHAIN_1/applications/$APPLICATION_ID"`.
- To query the service to perform an HTTP query locally:
```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APPLICATION_ID
query {
  performHttpRequest
}
```
- To make the service perform an HTTP query locally and use the response to propose a block:
```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APPLICATION_ID
mutation sendLocalHttpResponseToContract {}
```
- To make the contract perform an HTTP request:
```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APPLICATION_ID
mutation askContractToPerformHttpRequest {}
```
- To make the contract use the service as an oracle to perform an HTTP request:
```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APPLICATION_ID
mutation askContractToUseServiceForHttpRequest {}
```
