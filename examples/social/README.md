<!-- cargo-rdme start -->

# A Social Media Example Application

This example illustrates how to use channels for cross-chain messages.

For simplicity, each microchain represents one user—its owner. They can subscribe to other
users and make text posts that get sent to their subscribers.

## How it Works

The application's state on every microchain contains a set of posts created by this chain
owner, and a set of posts received from other chains that it has subscribed to. The
received posts are indexed by timestamp, sender and index.

There are `Subscribe` and `Unsubscribe` operations: If a chain owner includes these in a
new block, they can subscribe to or unsubscribe from another chain.

There is also a `Post` operation: It creates a new post and sends it to a channel, so that
it reaches all subscribers.

There are corresponding `RequestSubscribe`, `RequestUnsubscribe` and `Posts` cross-chain
message variants that are created when these operations are handled. The first two are
sent directly to the chain we want to subscribe to or unsubscribe from. The latter goes
to the channel.

## Usage

To try it out, first setup a local network with two wallets, and keep it running in a
separate terminal:

```bash
./scripts/run_local.sh
```

Compile the `social` example and create an application with it:

```bash
export LINERA_WALLET="$(realpath target/debug/wallet.json)"
export LINERA_STORAGE="rocksdb:$(dirname "$LINERA_WALLET")/linera.db"
export LINERA_WALLET_2="$(realpath target/debug/wallet_2.json)"
export LINERA_STORAGE_2="rocksdb:$(dirname "$LINERA_WALLET_2")/linera_2.db"

cd examples/social && cargo build --release && cd ../..

linera --wallet "$LINERA_WALLET" --storage "$LINERA_STORAGE" publish-and-create examples/target/wasm32-unknown-unknown/release/social_{contract,service}.wasm
```

This will output the new application ID, e.g.:

```rust
e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
```

With the `wallet show` command you can find the ID of the application creator's chain:

```bash
linera --wallet "$LINERA_WALLET" --storage "$LINERA_STORAGE" wallet show
```

```rust
e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
```


Now start a node service for each wallet, using two different ports:

```bash
linera --wallet "$LINERA_WALLET" --storage "$LINERA_STORAGE" service --port 8080 &
linera --wallet "$LINERA_WALLET_2" --storage "$LINERA_STORAGE_2" service --port 8081 &
```

Point your browser to http://localhost:8081. This is the wallet that didn't create the
application, so we have to request it from the creator chain:

```json
mutation {
    requestApplication(
        applicationId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000"
    )
}
```

Now in both http://localhost:8080 and http://localhost:8081, this should list the
application and provide a link to its GraphQL API:

```json
query { applications { id description link } }
```

Open both URLs under the entry `link`. Now you can use the application on each chain.
E.g. in the 8081 tab subscribe to the other chain:

```json
mutation {
    subscribe(
        chainId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
    )
}
```

Now make a post in the 8080 tab:

```json
mutation {
    post(
        text: "Linera Social is the new Mastodon!"
    )
}
```

Since 8081 is a subscriber. Let's see if it received any posts:

```json
query { receivedPostsKeys { timestamp author index } }
```

This should now list one entry, with timestamp, author and an index. If we view that
entry, we can see the posted text:

```json
query {
  receivedPosts(
    key: {
      timestamp: 1685626618522492,
      author: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65",
      index: 0
    }
  )
}
```

```json
{
  "data": {
    "receivedPosts": "Linera Social is the new Mastodon!"
  }
}
```

<!-- cargo-rdme end -->
