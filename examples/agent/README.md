# Linera Agent

This example application runs a Large Language Model (LLM) Agent which uses an LLM
to forge queries by introspecting the Linera Node Service GraphQL API.

## How it works

The agent wraps an LLM of choice and is provided with context on what Linera is and
the core concepts. Further context is provided by performing an introspection query
which injects the Linera Node Service GraphQL schema. The schema is quite large - 
the resulting context in ~15,000 tokens being used just for context.

After the context is provided, a chat is initiated between user and agent, where the
agent writes GraphQL queries on behalf of the user and uses the Linera Node Service 
tool to submit them.

## Usage

Assume a Linera Node Service is running on `localhost:8080` and is connected to a running
network (local or otherwise).

```bash
export OPENAI_API_KEY="..." # Get your OpenAI API key here: https://openai.com/index/openai-api/
cargo run
I'm the Linera agent. How can I help! (Type 'exit' to quit.)
> Hey! What's my default chain?
Req: query { chains { default } }
========================== Response ============================
{"data":{"chains":{"default":"01a7d54c59b43084ae49ebc490bb3c7c1f0924c18e93e62ecddfaf721c98e493"}},"errors":null}
================================================================
```
