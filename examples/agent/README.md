Linera Agent
This example application runs a Large Language Model (LLM) Agent that interacts with the Linera Node Service GraphQL API by generating queries through introspection.

How It Works
The agent wraps an LLM of choice and is initialized with context about Linera and its core concepts.
The agent performs an introspection query to retrieve and inject the Linera Node Service GraphQL schema.
Since the schema is large (~15,000 tokens), a significant portion of the LLM's context is dedicated to processing it.
Once the context is set, a chat session begins where the user can interact with the agent.
The agent generates GraphQL queries based on user input and submits them using the Linera Node Service.
Usage
Ensure that the Linera Node Service is running on localhost:8080 and is connected to an active network (local or otherwise).

1. Set Up Your OpenAI API Key
```bash
export OPENAI_API_KEY="your-api-key-here"
# Get your OpenAI API key: https://openai.com/index/openai-api/
```
2. Run the Agent
```bash
cargo run
```
3. Interact with the Agent
Once started, the agent will prompt you for input:
```bash
I'm the Linera agent. How can I help? (Type 'exit' to quit.)
> Hey! What's my default chain?
```
The agent then generates and submits a GraphQL query:
```bash
query { chains { default } }
```
Example Response:
```bash
{
  "data": {
    "chains": {
      "default": "01a7d54c59b43084ae49ebc490bb3c7c1f0924c18e93e62ecddfaf721c98e493"
    }
  },
  "errors": null
}
```
