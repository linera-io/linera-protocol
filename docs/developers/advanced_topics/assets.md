# Applications that Handle Assets

In general, if you send tokens to a chain owned by someone else, you rely on
them for asset availability: if they don't handle your messages, you don't have
access to your tokens.

Fortunately, Linera provides a solution based on temporary chains: if the number
of parties who want to participate is limited and known in advance, we can:

- make them all chain owners using the `linera change-ownership` command,
- allow only one application's operations on the chain,
- and allow only that operation to close the chain, using
  `linera change-application-permissions`.

Such an application should have a designated operation or message that causes it
to close the chain: when that operation is executed, it should send back all
remaining assets, and call the runtime's `close_chain` method.

Once the chain is closed, owners can still create blocks to reject messages.
That way, even assets that are in flight can be returned.

The
[`matching-engine` example application](https://github.com/linera-io/linera-protocol/tree/main/examples/matching-engine)
does this:

```rust,ignore
    async fn execute_operation(&mut self, operation: Operation) -> Self::Response {
        match operation {
            // ...
            Operation::CloseChain => {
                for order_id in self.state.orders.indices().await.unwrap() {
                    match self.modify_order(order_id, ModifyAmount::All).await {
                        Some(transfer) => self.send_to(transfer),
                        // Orders with amount zero may have been cleared in an earlier iteration.
                        None => continue,
                    }
                }
                self.runtime
                    .close_chain()
                    .expect("The application does not have permissions to close the chain.");
            }
        }
    }
```

This enables doing atomic swaps using the Matching Engine: if you make a bid,
you are guaranteed that at any point in time you can get back either the tokens
you are offering or the tokens you bought.
