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
use schemars::{schema_for, JsonSchema};
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
    let node_service =
        LineraNodeService::new(opt.node_service_url.parse().unwrap(), opt.model.clone()).unwrap();

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

#[derive(Debug, Deserialize, JsonSchema)]
struct LineraNodeServiceArgs {
    input: QueryType,
}

#[derive(Debug, Deserialize, JsonSchema)]
enum QueryType {
    QuerySystem {
        query: String,
    },
    QueryApplication {
        chain_id: String,
        application_id: String,
        query: String,
    },
}

#[derive(Serialize, Deserialize)]
struct LineraNodeServiceOutput {
    data: serde_json::Value,
    errors: Option<Vec<serde_json::Value>>,
}

struct LineraNodeService {
    url: Url,
    client: Client,
    model_name: String,
    ensemble_tx: tokio::sync::mpsc::Sender<EnsembleQuery>,
}

impl LineraNodeService {
    fn new(url: Url, model_name: String) -> Result<Self> {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(run_ensemble(rx));
        Ok(Self {
            url,
            client: Client::new(),
            model_name,
            ensemble_tx: tx,
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

impl Tool for LineraNodeService {
    const NAME: &'static str = "Linera";
    type Error = reqwest::Error;
    type Args = LineraNodeServiceArgs;
    type Output = LineraNodeServiceOutput;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: "Linera".to_string(),
            description: "Interact with a Linera wallet via GraphQL".to_string(),
            parameters: serde_json::to_value(schema_for!(LineraNodeServiceArgs)).unwrap(),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        eprintln!("Args: {:?}", args);
        match args.input {
            QueryType::QuerySystem { query } => {
                let response = self
                    .client
                    .post(self.url.clone())
                    .json(&json!({ "query": query }))
                    .send()
                    .await?;
                response.json().await
            }
            QueryType::QueryApplication {
                chain_id,
                application_id,
                query,
            } => {
                let (tx, rx) = tokio::sync::oneshot::channel::<String>();
                let ensemble_query = EnsembleQuery {
                    url: self.url.clone(),
                    chain_id,
                    application_id,
                    query,
                    model_name: self.model_name.clone(),
                    graphql_def: self.get_graphql_definition(self.url.clone()).await.unwrap(),
                    sender: tx,
                };
                self.ensemble_tx.send(ensemble_query).await.unwrap();
                let response = rx.await.unwrap();
                Ok(LineraNodeServiceOutput {
                    data: json!(response),
                    errors: None,
                })
            }
        }
    }
}

struct EnsembleQuery {
    url: Url,
    chain_id: String,
    application_id: String,
    query: String,
    model_name: String,
    graphql_def: String,
    sender: tokio::sync::oneshot::Sender<String>,
}

async fn run_ensemble(mut rx: tokio::sync::mpsc::Receiver<EnsembleQuery>) {
    while let Some(ensemble_query) = rx.recv().await {
        let linera_app_service = LineraApplicationService::new(
            ensemble_query.url,
            ensemble_query.chain_id,
            ensemble_query.application_id,
            Client::new(),
        );
        let openai = openai::Client::from_env();
        let model = openai.completion_model(&ensemble_query.model_name);
        let agent = AgentBuilder::new(model)
            .preamble(PREAMBLE)
            .context(LINERA_CONTEXT)
            .context(&ensemble_query.graphql_def)
            .tool(linera_app_service)
            .build();
        let mut backoff = tokio::time::Duration::from_secs(2);
        let mut attempts = 0;
        let response = loop {
            match agent.chat(ensemble_query.query.as_str(), vec![]).await {
                Ok(response) => break response,
                Err(e) => {
                    attempts += 1;
                    eprintln!(
                        "Error occurred: {}. Retrying in {} seconds...",
                        e,
                        backoff.as_secs()
                    );
                    if attempts >= 5 {
                        panic!("Failed after 5 attempts");
                    }
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        };

        if let Err(e) = ensemble_query.sender.send(response) {
            eprintln!("Error sending response from ensemble: {}", e);
        }
    }
}

#[derive(Debug, Deserialize)]
struct LineraApplicationServiceArgs {
    query: String,
}

struct LineraApplicationService {
    name: String,
    url: Url,
    client: Client,
}

impl LineraApplicationService {
    fn new(node_url: Url, chain_id: String, application_id: String, client: Client) -> Self {
        let name = format!("Application {} Tool", application_id);
        let url = node_url
            .join("chains")
            .unwrap()
            .join(&chain_id)
            .unwrap()
            .join("applications")
            .unwrap()
            .join(&application_id)
            .unwrap();
        Self { name, url, client }
    }
}

impl Tool for LineraApplicationService {
    const NAME: &'static str = "Linera Application Service";
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
