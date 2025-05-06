# Counter No GraphQL Example Application

This example application implements a simple counter contract, it is initialized with an
unsigned integer that can be increased by the `increment` operation. In contrast with the
counter application, it works without GraphQL.

## How It Works

It is a simple Linera application, which is initialized by a `u64` which can be incremented
by a `u64`.

For example, if the contract was initialized with 1, querying the contract would give us 1. Now if we want to
`increment` it by 3, we will have to perform an operation with the parameter being 3. Now querying the
application would give us 4 (1+3 = 4).

## Usage
