#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;
mod token;

use self::state::Llm;
use crate::token::TokenOutputStream;
use async_graphql::{EmptySubscription, Object, Schema};
use async_trait::async_trait;
use candle_core::{quantized::ggml_file, Device};
use candle_transformers::models::quantized_llama::ModelWeights;
use linera_sdk::{
    base::WithServiceAbi, views::RegisterView, Service, ServiceRuntime, ViewStateStorage,
};
use log::error;
use std::{io::Cursor, sync::Arc};
use thiserror::Error;
use tokenizers::Tokenizer;
use linera_sdk::abi::ServiceAbi;

linera_sdk::service!(Llm);

impl WithServiceAbi for Llm {
    type Abi = llm::LlmAbi;
}

struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn hello(&self) -> &str {
        "world!"
    }
}

struct QueryRoot {
    response: String,
}

#[Object]
impl QueryRoot {
    async fn response(&self) -> &str {
        &self.response
    }
}

#[async_trait]
impl Service for Llm {
    type Error = ServiceError;
    type Storage = ViewStateStorage<Self>;

    async fn handle_query(
        self: Arc<Self>,
        _runtime: &ServiceRuntime,
        request: Self::Query,
    ) -> Result<Self::QueryResponse, Self::Error> {
        let prompt_string = &request.query; // TODO is this correct?
        let raw_weights = self.model_weights.get().clone();
        let tokenizer_bytes = self.tokenizer.get();
        let gqa = 1; // group query attention, is 1 for llama.
        self.run_model(prompt_string, raw_weights, tokenizer_bytes, gqa)?;

        let schema = Schema::build(
            QueryRoot {
                response: "TODO".to_string(),
            },
            MutationRoot,
            EmptySubscription,
        )
        .finish();
        Ok(schema.execute(request).await)
    }
}

impl Llm {
    fn load_model(&self, model_weights: Vec<u8>, gqa: usize) -> Result<ModelWeights, ServiceError> {
        let model_contents =
            ggml_file::Content::read(&mut Cursor::new(model_weights), &Device::Cpu).unwrap();
        let mut total_size_in_bytes = 0;
        for (_, tensor) in model_contents.tensors.iter() {
            let elem_count = tensor.shape().elem_count();
            total_size_in_bytes +=
                elem_count * tensor.dtype().type_size() / tensor.dtype().block_size();
        }

        println!(
            "loaded {:?} tensors ({}B) ",
            model_contents.tensors.len(),
            total_size_in_bytes,
        );

        println!("params: {:?}", model_contents.hparams);

        Ok(ModelWeights::from_ggml(model_contents, gqa)?)
    }

    fn run_model(&self, prompt_string: &str, raw_weights: Vec<u8>, tokenizer_bytes: &Vec<u8>, gqa: usize) -> Result<(), ServiceError> {
        let model = self.load_model(raw_weights, gqa)?;
        let tokenizer = Tokenizer::from_bytes(tokenizer_bytes)
            .map_err(|e| ServiceError::Tokenizer(format!("{}", e)))?;
        let mut token_output_stream = TokenOutputStream::new(tokenizer);
        let tokens = token_output_stream
            .tokenizer()
            .encode(prompt_string, true)
            .map_err(|e| ServiceError::Tokenizer(format!("{}", e)))?;
        todo!();
        Ok(())
    }
}

/// An error that can occur while querying the service.
#[derive(Debug, Error)]
pub enum ServiceError {
    /// Query not supported by the application.
    #[error("Queries not supported by application")]
    QueriesNotSupported,

    /// Invalid query argument; could not deserialize request.
    #[error("Invalid query argument; could not deserialize request")]
    InvalidQuery(#[from] serde_json::Error),
    // Add error variants here.
    /// Invalid query argument; could not deserialize request.
    #[error("Candle error")]
    Candle(#[from] candle_core::Error),

    #[error("Tokenizer error")]
    Tokenizer(String),
}
