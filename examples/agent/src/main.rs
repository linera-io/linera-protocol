// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::io::{self, Write};

use anyhow::Result;
use clap::Parser;
use reqwest::Client;
use rig::{
    agent::{Agent, AgentBuilder},
    completion::{Chat, CompletionModel, Message, ToolDefinition},
    providers::openai,
    tool::Tool,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use url::Url;

const INTROSPECTION_QUERY: &str = r#"
query IntrospectionQuery {
  __schema {
    queryType {
      name
    }
    mutationType {
      name
    }
    types {
      ...FullType
    }
  }
}

fragment FullType on __Type {
  kind
  name
  description
  fields(includeDeprecated: false) {
    name
    description
    args {
      ...InputValue
    }
    type {
      ...TypeRef
    }
  }
  inputFields {
    ...InputValue
  }
  interfaces {
    ...TypeRef
  }
  enumValues(includeDeprecated: true) {
    name
    description
  }
  possibleTypes {
    ...TypeRef
  }
}

fragment InputValue on __InputValue {
  name
  description
  type {
    ...TypeRef
  }
  defaultValue
}

fragment TypeRef on __Type {
  kind
  name
  ofType {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"#;

const PREAMBLE: &str = r#"
You are a bot that works and interacts with the Linera blockchain via a Linera wallet.

Even though you're going to be using mostly GraphQL, try to use natural language as much as possible to speak to the user.

If you are not certain on a given instruction, ask for clarification.
"#;

const LINERA_CONTEXT: &str = r#"
Linera is a decentralized infrastructure optimized for Web3 applications requiring guaranteed performance for unlimited active users. It introduces "microchains," lightweight blockchains running in parallel within a single validator set. Each user operates their own microchain, adding blocks as needed, which ensures efficient, elastic scalability.

In Linera, user wallets manage their microchains, allowing owners to control block additions and contents. Validators ensure block validity, verifying operations like asset transfers and incoming messages. Applications are WebAssembly (Wasm) programs with distinct states on each chain, facilitating asynchronous cross-chain communication.

Linera's architecture is ideal for applications requiring real-time transactions among numerous users, such as micro-payments, social data feeds, and turn-based games. By embedding full nodes into user wallets, typically as browser extensions, Linera enables direct state queries and enhances security without relying on external APIs or light clients.
"#;

#[derive(Parser, Debug)]
#[command(
    name = "Linera Agent",
    about = "An AI agent which uses LLMs to interface with Linera."
)]
struct Opt {
    /// OpenAI model the agent should use.
    #[arg(long, default_value = "gpt-4o")]
    model: String,

    /// Linera node service URL.
    #[arg(long, default_value = "http://localhost:8080")]
    node_service_url: String,
}

#[tokio::main]
async fn main() {
    let opt = Opt::parse();

    let openai = openai::Client::from_env();
    let model = openai.completion_model(&opt.model);
    let node_service = LineraNodeService::<openai::CompletionModel>::new(
        opt.node_service_url.parse().unwrap(),
        model.clone(),
    )
    .unwrap();

    let system_graphql_def = node_service.system_graphql_definition().await.unwrap();
    let graphql_context = format!(
        "This is the GraphQL schema for interfacing with the Linera service: {}",
        system_graphql_def
    );

    // Configure the agent
    let agent = AgentBuilder::new(model.clone())
        .preamble(PREAMBLE)
        .context(LINERA_CONTEXT)
        .context(&graphql_context)
        .tool(node_service)
        .build();

    chat(agent).await;
}

#[derive(Debug, Deserialize)]
#[serde(tag = "tag ")]
enum LineraNodeServiceArgs {
    QuerySystem {
        query: String,
    },
    AddApplication {
        application_id: String,
    },
    QueryApplication {
        application_id: String,
        query: String,
    },
}

#[derive(Serialize, Deserialize)]
struct LineraNodeServiceOutput {
    data: serde_json::Value,
    errors: Option<Vec<serde_json::Value>>,
}

struct LineraNodeService<M: CompletionModel> {
    url: Url,
    client: Client,
    model: M,
}

impl<M: CompletionModel> LineraNodeService<M> {
    fn new(url: Url, model: M) -> Result<Self> {
        Ok(Self {
            url,
            client: Client::new(),
            model,
        })
    }

    async fn system_graphql_definition(&self) -> Result<String> {
        self.get_graphql_definition(self.url.clone()).await
    }

    async fn get_graphql_definition(&self, url: Url) -> Result<String> {
        let response = self
            .client
            .post(url)
            .json(&json!({
                "operationName": "IntrospectionQuery",
                "query": INTROSPECTION_QUERY
            }))
            .send()
            .await?;
        Ok(response.text().await?)
    }
}

impl<M: CompletionModel> Tool for LineraNodeService<M> {
    const NAME: &'static str = "Linera";
    type Error = reqwest::Error;
    type Args = LineraNodeServiceArgs;
    type Output = LineraNodeServiceOutput;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: "Linera".to_string(),
            description: "Interact with a Linera wallet via GraphQL".to_string(),
            parameters: json!({
                
                    "tag": { "type": "object", "oneOf": [
                        { "properties": { "query": { "type": "string" } }, "required": ["query"] },
                        { "properties": { "application_id": { "type": "string" } }, "required": ["application_id"] },
                        { "properties": { "application_id": { "type": "string" }, "query": { "type": "string" } }, "required": ["application_id", "query"] }
                    ]}
                
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        eprintln!("Args: {:?}", args);
        match args {
            LineraNodeServiceArgs::QuerySystem { query } => {
                let response = self
                    .client
                    .post(self.url.clone())
                    .json(&json!({ "query": query }))
                    .send()
                    .await?;
                response.json().await
            }
            LineraNodeServiceArgs::AddApplication { application_id } => {
                unimplemented!();
            }
            LineraNodeServiceArgs::QueryApplication {
                application_id,
                query,
            } => {
                let url = self.url.join(&application_id).unwrap();
                let graphql_def = self.get_graphql_definition(url).await.unwrap();
                let linera_app_service = LineraApplicationService::new(self.url.clone(), application_id, self.client.clone());  
                println!("Got here.");
                let agent = AgentBuilder::new(self.model.clone())
                    .preamble(PREAMBLE)
                    .context(LINERA_CONTEXT)
                    .context(&graphql_def)
                    .tool(linera_app_service)
                    .build();
                let runtime = tokio::runtime::Runtime::new().unwrap();
                let response =
                    runtime.block_on(async { agent.chat(query.as_str(), vec![]).await.unwrap() });
                Ok(LineraNodeServiceOutput {
                    data: json!(response),
                    errors: None,
                })
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct LineraApplicationServiceArgs {
    query: String,
}

struct LineraApplicationService {
    url: Url,
    client: Client,
}

impl LineraApplicationService {
    fn new(node_url: Url, application_id: String, client: Client) -> Self {
        Self {
            url: node_url.join(&application_id).unwrap(),
            client,
        }
    }
}

impl Tool for LineraApplicationService {
    const NAME: &'static str = ""; // Don't need this.
    type Error = reqwest::Error;
    type Args = LineraApplicationServiceArgs; // GraphQL
    type Output = LineraNodeServiceOutput;

    fn definition(
        &self,
        _prompt: String,
    ) -> impl std::future::Future<Output = ToolDefinition> + Send + Sync {
        async move {
            ToolDefinition {
                name: Self::NAME.to_string(),
                description: "Interact with Linera applications.".to_string(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The query to send to the application. This *must* be valid GraphQL and not JSON. Do not escape quotes."
                        }
                    }
                }),
            }
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
      let query = args.query;
      let response = self
      .client
      .post(self.url.clone())
      .json(&json!({ "query": query }))
      .send()
      .await?;
  response.json().await
    }
}

pub async fn chat(chatbot: impl Chat) {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut chat_log = vec![];

    println!("I'm the Linera agent. How can I help! (Type 'exit' to quit.)");
    loop {
        print!("> ");
        // Flush stdout to ensure the prompt appears before input
        stdout.flush().unwrap();

        let mut input = String::new();
        match stdin.read_line(&mut input) {
            Ok(_) => {
                // Remove the newline character from the input
                let input = input.trim();
                if input == "exit" {
                    break;
                }

                match chatbot.chat(input, chat_log.clone()).await {
                    Ok(response) => {
                        chat_log.push(Message::user(input));
                        chat_log.push(Message::assistant(response.clone()));

                        println!(
                            "========================== Response ============================"
                        );
                        println!("{response}");
                        println!(
                            "================================================================\n\n"
                        );
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        continue;
                    }
                }
            }
            Err(error) => println!("Error reading input: {}", error),
        }
    }
}
