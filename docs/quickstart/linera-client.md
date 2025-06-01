# linera-client Quickstart

## What it does
Library for building Linera clients, used by CLI wallets and node services.

## Basic Example

```rust
use linera_base::{data_types::Amount, identifiers::ChainId};

// Example client operations
pub struct BasicClientExample {
    chain_id: ChainId,
}

impl BasicClientExample {
    pub fn new(chain_id: ChainId) -> Self {
        Self { chain_id }
    }

    // Query balance example
    pub async fn query_balance(&self) -> Result<Amount, Box<dyn std::error::Error>> {
        // Implementation would use actual client context
        // This is a simplified example
        Ok(Amount::ZERO)
    }

    // Transfer example structure
    pub async fn transfer(
        &self,
        amount: Amount,
        to_chain: ChainId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Implementation would use actual client context
        println!("Transfer {} to chain {}", amount, to_chain);
        Ok(())
    }
}
