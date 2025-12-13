# Calling other Applications

We have seen that cross-chain messages sent by an application on one chain are
always handled by the _same_ application on the target chain.

This section is about calling other applications using _cross-application
calls_.

Such calls happen on the same chain and are made with the helper method
[`ContractRuntime::call_application`](https://docs.rs/linera-sdk/latest/linera_sdk/contract/struct.ContractRuntime.html#method_call_application):

```rust,ignore
{{#include ../../../linera-sdk/src/contract/runtime.rs:call_application}}
```

The `authenticated` argument specifies whether the callee is allowed to perform
actions that require authentication either

- on behalf of the signer of the original block that caused this call, or
- on behalf of the calling application.

The `application` argument is the callee's application ID, and `A` is the
callee's ABI.

The `call` argument is the operation requested by the application call.

## Example: crowd-funding

The `crowd-funding` example application allows the application creator to launch
a campaign with a funding target. That target can be an amount specified in any
type of token based on the `fungible` application. Others can then pledge tokens
of that type to the campaign, and if the target is not reached by the deadline,
they are refunded.

If Alice used the `fungible` example to create a Pugecoin application (with an
impressionable pug as its mascot), then Bob can create a `crowd-funding`
application, use Pugecoin's application ID as `CrowdFundingAbi::Parameters`, and
specify in `CrowdFundingAbi::InstantiationArgument` that his campaign will run
for one week and has a target of 1000 Pugecoins.

Now let's say Carol wants to pledge 10 Pugecoin tokens to Bob's campaign. She
can make her pledge by running the `linera service` and making a query to Bob's
application:

```json
mutation { pledge(owner: "User:841…6c0", amount: "10") }
```

This will add a block to Carol's chain containing the pledge operation that gets
handled by `CrowdFunding::execute_operation`, resulting in one cross-application
call and two cross-chain messages:

First `CrowdFunding::execute_operation` calls the `fungible` application on
Carol's chain to transfer 10 tokens to Carol's account on Bob's chain:

```rust,ignore
// ...
let call = fungible::Operation::Transfer {
    owner,
    amount,
    target_account,
};
// ...
self.runtime
    .call_application(/* authenticated by owner */ true, fungible_id, &call);
```

This causes `Fungible::execute_operation` to be run, which will create a
cross-chain message sending the amount 10 to the Pugecoin application instance
on Bob's chain.

After the cross-application call returns, `CrowdFunding::execute_operation`
continues to create another cross-chain message
`crowd_funding::Message::PledgeWithAccount`, which informs the crowd-funding
application on Bob's chain that the 10 tokens are meant for the campaign.

When Bob now adds a block to his chain that handles the two incoming messages,
first `Fungible::execute_message` gets executed, and then
`CrowdFunding::execute_message`. The latter makes another cross-application call
to transfer the 10 tokens from Carol's account to the crowd-funding
application's account (both on Bob's chain). That is successful because Carol
does now have 10 tokens on this chain and she authenticated the transfer
indirectly by signing her block. The crowd-funding application now makes a note
in its application state on Bob's chain that Carol has pledged 10 Pugecoin
tokens.

# References

For the complete code, please take a look at the
[`crowd-funding`](https://github.com/linera-io/linera-protocol/blob/{{#include
../../RELEASE_HASH}}/examples/crowd-funding/src/contract.rs) and the
[`fungible`](https://github.com/linera-io/linera-protocol/blob/{{#include
../../RELEASE_HASH}}/examples/fungible/src/contract.rs) application contracts in
the `examples` folder in `linera-protocol`.

The implementation of the Runtime made available to contracts is defined in
[this file](https://github.com/linera-io/linera-protocol/blob/{{#include
../../RELEASE_HASH}}/linera-sdk/src/contract/runtime.rs).
