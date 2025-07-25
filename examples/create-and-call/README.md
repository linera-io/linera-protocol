# Create and call Example Application

This example demonstrates an example where a contract creates another contract and calls it.
The example application being created is the counter-no-graphql. The deployment can be seen
in the `test_create_and_call_end_to_end` end-to-end test.

## Note

In the block in which `publish_module`, `create_application` and `call_application` are done,
we cannot call the `query_application` cannot be called since the service are about the previous
block.

## Usage
