# Creating New Blocks

> In Linera, the responsibility of proposing blocks is separate from the task of
> validating blocks.

While all chains are validated in the same way, the Linera protocol defines
several types of chains, depending on how new blocks are produced.

- The simplest and lowest-latency type of chain is called _single-owner_ chain.

- Other types of Linera chains not currently supported in the SDK include
  _multi-user chains_ and _public chains_ (see the
  [whitepaper](https://linera.io/whitepaper) for more context).

> For most types of chains (all but _public chains_), Linera validators do not
> need to exchange messages with each other.

Instead, the wallets (aka. `linera` clients) of chain owners make the system
progress by proposing blocks and actively providing any additional required data
to the validators. For instance, client commands such as `transfer`,
`publish-module`, or `open-chain` perform multiple steps to append a block
containing the token transfer, application publishing, or chain creation
operation:

- The Linera client creates a new block containing the desired operation and new
  incoming messages, if there are any. It also contains the most recent block's
  hash to designate its parent. The client sends the new block to all
  validators.

- The validators validate the block, i.e. check that the block satisfies the
  conditions listed above, and send a cryptographic signature to the client,
  indicating that they vote to append the new block. But only if they have not
  voted for a different block on the same height earlier!

- The client ideally receives a vote from every validator, but only a quorum of
  votes (say, two thirds) are required: These constitute a "certificate",
  proving that the block was confirmed. The client sends the certificate to
  every validator.

- The validators "execute" the block: They update their own view of the most
  recent state of the chain by applying all messages and operations, and if it
  generated any cross-chain messages, they send these to the appropriate
  workers.

To guarantee that each incoming message in a block was actually sent by another
chain, a validator will, in the second step, only _vote_ for a block if it has
already executed the block that sent it. However, when receiving a valid
certificate for a block that receives a message it has not seen yet, it will
accept and _execute_ the block anyway. The certificate is proof that most other
validators have seen the message, so it must be correct.

In the case of single-owner chains, clients must be carefully implemented so
that they never propose multiple blocks at the same height. Otherwise, the chain
may be stuck: once each of the two conflicting blocks has been signed by enough
validators, it becomes impossible to collect a quorum of votes for either block.

In the future, we anticipate that most users will use _multi-user chains_ even
if they are the only owners of their chains. Multi-user chains have two
confirmation steps instead of one, but it is not possible to accidentally make a
chain unextendable. They also allow users to delegate certain administrative
tasks to third-parties, notably to help with epoch changes (i.e. when the
validators change if reconfigured).
