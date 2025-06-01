# linera-sdk Quickstart

## What it does
The Linera SDK for building Web3 applications in Rust that compile to WebAssembly.

## Basic Example

```rust
use linera_sdk::{
    base::{ApplicationId, ChainId},
    Contract, ExecutionResult, Service,
};
use serde::{Deserialize, Serialize};

// Define your application state
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct MyAppState {
    pub counter: u64,
}

// Define operations
#[derive(Debug, Deserialize, Serialize)]
pub enum Operation {
    Increment,
    Decrement,
}

// Basic contract implementation example
impl MyAppState {
    pub fn increment(&mut self) {
        self.counter += 1;
    }

    pub fn decrement(&mut self) {
        if self.counter > 0 {
            self.counter -= 1;
        }
    }
}
