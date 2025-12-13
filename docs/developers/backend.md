# Writing Linera Applications

In this section, we'll be exploring how to create Web3 applications using the
Linera SDK.

We'll use a simple "counter" application as a running example.

We'll focus on the backend of the application, which consists of two main parts:
a _smart contract_ and its GraphQL service.

Both the contract and the service of an application are written in Rust using
the crate [`linera-sdk`](https://crates.io/crates/linera-sdk), and compiled to
Wasm bytecode.

This section should be seen as a guide versus a reference manual for the SDK.
For the reference manual, refer to the
[documentation of the crate](https://docs.rs/linera-sdk/latest/linera_sdk/).
