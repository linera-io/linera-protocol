# Contract Finalization

When a block finishes executing successfully, there's a final step where all
loaded application contracts are finalized. Finalization happens in two stages:

1. **`Contract::save`** is called on each loaded contract. This flushes the
   contract's in-memory state to storage. `save` is also called before
   checkpoints during block execution, so it may run multiple times.

2. **`Contract::terminate`** is called once, consuming the contract instance.
   This is the place to perform final validation. The default implementation
   is empty, so contracts only need to override it when they have invariants
   to enforce.

During finalization, contracts may read their own state but are **not** allowed
to call other applications, because they are all also in the process of
finalizing.

If `terminate` panics, the block is rejected even if every operation and message
in the block had succeeded. This allows a contract to reject blocks that violate
invariants established during cross-application calls.

As an example, a contract that executes a cross-application call with
`Operation::StartSession` may require the same caller to perform another
cross-application call with `Operation::EndSession` before the block ends.

```rust,edition2021
# extern crate serde;
# extern crate linera_sdk;
# use serde::{Deserialize, Serialize};
# use linera_sdk::linera_base_types::*;
# use linera_sdk::*;
# use linera_sdk::abi::*;
# use std::collections::HashSet;
# use linera_sdk::views::{linera_views, RegisterView, RootView, ViewStorageContext};
# use crate::linera_sdk::views::View as _;
# use linera_sdk::linera_base_types::ApplicationId;

#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct MyState {
    pub value: RegisterView<u64>,
    // ...
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Operation { StartSession, EndSession }

pub struct MyAbi;

impl ContractAbi for MyAbi {
    type Operation = Operation;
    type Response = ();
}

pub struct MyContract {
    state: MyState,
    runtime: ContractRuntime<Self>,
    active_sessions: HashSet<ApplicationId>,
}

impl WithContractAbi for MyContract {
    type Abi = MyAbi;
}

impl Contract for MyContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = MyState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");

        MyContract {
            state,
            runtime,
            active_sessions: HashSet::new(),
        }
    }

    async fn instantiate(&mut self, (): Self::InstantiationArgument) {}

    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response {
        let caller_id = self.runtime
            .authenticated_caller_id()
            .expect("Missing caller ID");

        match operation {
            Operation::StartSession => {
                assert!(
                    self.active_sessions.insert(caller_id),
                    "Can't start more than one session for the same caller"
                );
            }
            Operation::EndSession => {
                assert!(
                    self.active_sessions.remove(&caller_id),
                    "Session was not started"
                );
            }
        }
    }

    async fn execute_message(&mut self, message: Self::Message) {
        unreachable!("This example doesn't support messages");
    }

    async fn save(&mut self) {
        self.state.save().await.expect("Failed to save state");
    }

    async fn terminate(self) {
        assert!(
            self.active_sessions.is_empty(),
            "Some sessions have not ended"
        );
    }
}
```
