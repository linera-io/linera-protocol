#![cfg_attr(target_arch = "wasm32", no_main)]

mod random;
mod state;
mod token;

use self::state::Llm;
use crate::token::TokenOutputStream;
use async_graphql::{EmptySubscription, Object, Response, Schema};
use async_trait::async_trait;
use candle_core::{quantized::ggml_file, Device, Tensor};
use candle_transformers::{
    generation::LogitsProcessor,
    models::{quantized_llama as model, quantized_llama::ModelWeights},
};
use linera_sdk::{
    abi::ServiceAbi, base::WithServiceAbi, views::RegisterView, Service, ServiceRuntime,
    ViewStateStorage,
};
use log::error;
use std::{io::Cursor, sync::Arc};
use async_graphql::Request;
use thiserror::Error;
use tokenizers::Tokenizer;

pub struct LlmService {
    state: Llm
}

linera_sdk::service!(LlmService);

impl WithServiceAbi for LlmService {
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

impl Service for LlmService {
    type Error = ServiceError;
    type Storage = ViewStateStorage<Self>;
    type State = Llm;
    type Parameters = ();

    async fn new(state: Self::State, _runtime: ServiceRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(LlmService { state })
    }

    async fn handle_query(&self, request: Request) -> Result<Response, Self::Error> {
        let prompt_string = &request.query; // TODO is this correct?
        let raw_weights = self.state.model_weights.get().clone();
        let tokenizer_bytes = self.state.tokenizer.get();
        let gqa = 1; // group query attention, is 1 for llama.
        self.state.run_model(prompt_string, raw_weights, tokenizer_bytes, gqa)?;

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

    // Copied mostly from https://github.com/huggingface/candle/blob/57267cd53612ede04090853680125b17956804f3/candle-examples/examples/quantized/main.rs
    fn run_model(
        &self,
        prompt_string: &str,
        raw_weights: Vec<u8>,
        tokenizer_bytes: &Vec<u8>,
        gqa: usize,
    ) -> Result<String, ServiceError> {
        let mut output = String::new();
        let mut model = self.load_model(raw_weights, gqa)?;
        let mut pre_prompt_tokens = vec![];

        let tokenizer = Tokenizer::from_bytes(tokenizer_bytes)
            .map_err(|e| ServiceError::Tokenizer(format!("{}", e)))?;
        let mut token_output_stream = TokenOutputStream::new(tokenizer);
        let tokens = token_output_stream
            .tokenizer()
            .encode(prompt_string, true)
            .map_err(|e| ServiceError::Tokenizer(format!("{}", e)))?;

        let prompt_tokens = [&pre_prompt_tokens, tokens.get_ids()].concat();
        let to_sample = 1000usize.saturating_sub(1); // 1000 is the default value in candle example
        let prompt_tokens = if prompt_tokens.len() + to_sample > model::MAX_SEQ_LEN - 10 {
            let to_remove = prompt_tokens.len() + to_sample + 10 - model::MAX_SEQ_LEN;
            prompt_tokens[prompt_tokens.len().saturating_sub(to_remove)..].to_vec()
        } else {
            prompt_tokens
        };
        let mut all_tokens = vec![];
        let seed = 299792458; // taken as the default value from the candle example.
        let mut logits_processor = LogitsProcessor::new(seed, None, None);

        let start_prompt_processing = std::time::Instant::now();
        let mut next_token = 0;
        for (pos, token) in prompt_tokens.iter().enumerate() {
            let input = Tensor::new(&[*token], &Device::Cpu)?.unsqueeze(0)?;
            let logits = model.forward(&input, pos)?;
            let logits = logits.squeeze(0)?;
            next_token = logits_processor.sample(&logits)?
        }

        all_tokens.push(next_token);
        if let Some(t) = token_output_stream.next_token(next_token)? {
            output.push_str(&t);
        }

        let eos_token = "</s>";
        let eos_token = *token_output_stream
            .tokenizer()
            .get_vocab(true)
            .get(eos_token)
            .unwrap();
        let start_post_prompt = std::time::Instant::now();
        let mut sampled = 0;
        let repeat_penatly = 1.1; // taken from candle example
        let repeat_last_n = 64; // taken from candle example
        for index in 0..to_sample {
            let input = Tensor::new(&[next_token], &Device::Cpu)?.unsqueeze(0)?;
            let logits = model.forward(&input, prompt_tokens.len() + index)?;
            let logits = logits.squeeze(0)?;

            let start_at = all_tokens.len().saturating_sub(repeat_last_n);
            candle_transformers::utils::apply_repeat_penalty(
                &logits,
                repeat_penatly,
                &all_tokens[start_at..],
            )?;

            next_token = logits_processor.sample(&logits)?;
            all_tokens.push(next_token);
            if let Some(t) = token_output_stream.next_token(next_token)? {
                output.push_str(&t);
            }
            sampled += 1;
            if next_token == eos_token {
                break;
            };
        }
        if let Some(rest) = token_output_stream
            .decode_rest()
            .map_err(|e| ServiceError::Candle(e))?
        {
            output.push_str(&rest)
        }
        Ok(output)
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
