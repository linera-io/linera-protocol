// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// TODO the following documentation involves `sleep`ing to avoid some race conditions.  See:
// - https://github.com/linera-io/linera-protocol/issues/1176
// - https://github.com/linera-io/linera-protocol/issues/1177

/*!
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

There are corresponding `Subscribe`, `Unsubscribe` and `Posts` cross-chain
message variants that are created when these operations are handled. The first two are
sent directly to the chain we want to subscribe to or unsubscribe from. The latter goes
to the channel.

## Usage

To try it out, build Linera and add it to your path:

```bash
cargo build
PATH=$PWD/target/debug:$PATH
```

Then, using the helper function defined by `linera net helper`, set up a local network
with two wallets and define variables holding their wallet paths (`$LINERA_WALLET_0`,
`$LINERA_WALLET_1`) and storage paths (`$LINERA_STORAGE_0`, `$LINERA_STORAGE_1`).

```bash
eval "$(linera net helper)"
linera_spawn_and_read_wallet_variables \
    linera net up \
        --extra-wallets 1
```

Compile the `social` example and create an application with it:

```bash
linera --with-wallet 0 project publish-and-create examples/social
```

This will output the new application ID, e.g.:

```ignore
e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
```

With the `wallet show` command you can find the ID of the application creator's chain:

```bash
linera --with-wallet 0 wallet show
```

```ignore
e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
```

Now start a node service for each wallet, using two different ports:

```bash
linera --with-wallet 0 service --port 8080 &

# Wait for it to complete
sleep 2

linera --with-wallet 1 service --port 8081 &

# Wait for it to complete
sleep 2
```

Point your browser to http://localhost:8081. This is the wallet that didn't create the
application, so we have to request it from the creator chain. As the chain ID specify the
one of the chain where it isn't registered yet:

```gql,uri=http://localhost:8081
mutation {
    requestApplication(
        chainId: "1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03",
        applicationId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000"
    )
}
```

Now in both http://localhost:8080 and http://localhost:8081, this should list the
application and provide a link to its GraphQL API. Remember to use each wallet's chain ID:

```gql,uri=http://localhost:8081
query {
    applications(
        chainId: "1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03"
    ) {
        id
        link
    }
}
```

Open both URLs under the entry `link`. Now you can use the application on each chain.
E.g. [in the 8081 tab](http://localhost:8081/chains/1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000)
subscribe to the other chain:

```gql,uri=http://localhost:8081/chains/1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
mutation {
    subscribe(
        chainId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
    )
}
```

Now make a post [in the 8080 tab](http://localhost:8080/chains/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000):

```gql,uri=http://localhost:8080/chains/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
mutation {
    post(
        text: "Linera Social is the new Mastodon!"
    )
}
```

Since 8081 is a subscriber. Let's see if it received any posts:

```gql,uri=http://localhost:8081/chains/1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
query { receivedPosts { keys { timestamp author index } } }
```

This should now list one entry, with timestamp, author and an index. If we view that
entry, we can see the posted text:

```gql
query {
    receivedPosts {
        entry(
            key: {
                timestamp: 1705504131018960,
                author: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65",
                index: 0
            }
        ) { value }
    }
}
```

```json
{
    "data": {
        "receivedPosts": {
            "entry": {
                "value": "Linera Social is the new Mastodon!"
            }
        }
    }
}
```
*/

use async_graphql::{InputObject, Request, Response, SimpleObject};
use linera_sdk::{
    base::{ChainId, ContractAbi, ServiceAbi, Timestamp},
    graphql::GraphQLMutationRoot,
    views::{CustomSerialize, ViewError},
};
use serde::{Deserialize, Serialize};

pub struct SocialAbi;

impl ContractAbi for SocialAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for SocialAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// An operation that can be executed by the application.
#[derive(Debug, Serialize, Deserialize, GraphQLMutationRoot)]
pub enum Operation {
    /// Request to be subscribed to another chain.
    Subscribe { chain_id: ChainId },
    /// Request to be unsubscribed from another chain.
    Unsubscribe { chain_id: ChainId },
    /// Send a new post to everyone who subscribed to us.
    Post { text: String },
}

/// A message of the application on one chain, to be handled on another chain.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    /// The origin chain wants to subscribe to the target chain.
    Subscribe,
    /// The origin chain wants to unsubscribe from the target chain.
    Unsubscribe,
    /// The origin chain made a post, and the target chain is subscribed.
    /// This includes the most recent posts in reverse order, and the total count of posts by the
    /// sender. I.e. the indices of the posts in the `Vec` are `count - 1, count - 2, ...`.
    Posts { count: u64, posts: Vec<OwnPost> },
}

/// A post's text and timestamp, to use in contexts where author and index are known.
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, SimpleObject)]
pub struct OwnPost {
    /// The timestamp of the block in which the post operation was included.
    pub timestamp: Timestamp,
    /// The posted text.
    pub text: String,
}

/// A post on the social app.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct Post {
    /// The key identifying the post, including the timestamp, author and index.
    pub key: Key,
    /// The post's text content.
    pub text: String,
}

/// A key by which a post is indexed.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, SimpleObject, InputObject)]
#[graphql(input_name = "KeyInput")]
pub struct Key {
    /// The timestamp of the block in which the post was included on the author's chain.
    pub timestamp: Timestamp,
    /// The owner of the chain on which the `Post` operation was included.
    pub author: ChainId,
    /// The number of posts by that author before this one.
    pub index: u64,
}

// Serialize keys so that the lexicographic order of the serialized keys corresponds to reverse
// chronological order, then sorted by author, then by descending index.
impl CustomSerialize for Key {
    fn to_custom_bytes(&self) -> Result<Vec<u8>, ViewError> {
        let data = (
            (!self.timestamp.micros()).to_be_bytes(),
            &self.author,
            (!self.index).to_be_bytes(),
        );
        Ok(bcs::to_bytes(&data)?)
    }

    fn from_custom_bytes(short_key: &[u8]) -> Result<Self, ViewError> {
        let (time_bytes, author, idx_bytes) = (bcs::from_bytes(short_key))?;
        Ok(Self {
            timestamp: Timestamp::from(!u64::from_be_bytes(time_bytes)),
            author,
            index: !u64::from_be_bytes(idx_bytes),
        })
    }
}

#[cfg(test)]
mod tests {
    use linera_sdk::{
        base::{ChainId, Timestamp},
        views::CustomSerialize,
    };

    use super::Key;

    #[test]
    fn test_key_custom_serialize() {
        let key = Key {
            timestamp: Timestamp::from(0x123456789ABCDEF),
            author: ChainId([0x12345, 0x6789A, 0xBCDEF, 0x0248A].into()),
            index: 0xFEDCBA9876543210,
        };
        let ser_key = key
            .to_custom_bytes()
            .expect("serialization of Key should succeed");
        let deser_key =
            Key::from_custom_bytes(&ser_key).expect("deserialization of Key should succeed");
        assert_eq!(key, deser_key);
    }
}
