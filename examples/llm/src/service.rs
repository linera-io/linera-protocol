// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod random;
mod state;
mod token;

use std::io::{Cursor, Seek, SeekFrom};

use async_graphql::{Context, EmptyMutation, EmptySubscription, Object, Request, Response, Schema};
use candle_core::{
    quantized::{ggml_file, gguf_file},
    Device, IndexOp, Tensor,
};
use candle_transformers::{
    generation::LogitsProcessor,
    models::{llama2_c, llama2_c::Llama, llama2_c_weights, quantized_llama::ModelWeights},
};
use linera_sdk::{base::WithServiceAbi, EmptyState, Service, ServiceRuntime};
use log::info;
use tokenizers::Tokenizer;

use crate::token::TokenOutputStream;

pub struct LlmService {
    runtime: ServiceRuntime<Self>,
}

linera_sdk::service!(LlmService);

impl WithServiceAbi for LlmService {
    type Abi = llm::LlmAbi;
}

struct QueryRoot {}

#[Object]
impl QueryRoot {
    async fn prompt(&self, ctx: &Context<'_>, prompt: String) -> String {
        let model_context = ctx.data::<ModelContext>().unwrap();
        model_context.run_model(&prompt).unwrap()
    }
}

enum Model {
    Llama {
        model: Llama,
        cache: llama2_c::Cache,
    },
    Qllama(ModelWeights),
}

impl Model {
    fn forward(&mut self, input: &Tensor, index_pos: usize) -> Result<Tensor, candle_core::Error> {
        match self {
            Model::Llama {
                model: llama,
                cache,
            } => llama.forward(input, index_pos, cache),
            Model::Qllama(model) => model.forward(input, index_pos),
        }
    }
}

struct ModelContext {
    model: Vec<u8>,
    tokenizer: Vec<u8>,
}

impl Service for LlmService {
    type State = EmptyState;
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        LlmService { runtime }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let query_string = &request.query;
        info!("query: {}", query_string);
        let raw_weights = self.runtime.fetch_url("http://localhost:10001/model.bin");
        info!("got weights: {}B", raw_weights.len());
        let tokenizer_bytes = self
            .runtime
            .fetch_url("http://localhost:10001/tokenizer.json");
        let model_context = ModelContext {
            model: raw_weights,
            tokenizer: tokenizer_bytes,
        };
        let schema = Schema::build(QueryRoot {}, EmptyMutation, EmptySubscription)
            .data(model_context)
            .finish();
        schema.execute(request).await
    }
}

impl ModelContext {
    fn try_load_gguf(cursor: &mut Cursor<Vec<u8>>) -> Result<ModelWeights, candle_core::Error> {
        info!("trying to load model assuming gguf");
        let model_contents = gguf_file::Content::read(cursor)?;
        let mut total_size_in_bytes = 0;
        for (_, tensor) in model_contents.tensor_infos.iter() {
            let elem_count = tensor.shape.elem_count();
            total_size_in_bytes +=
                elem_count * tensor.ggml_dtype.type_size() / tensor.ggml_dtype.block_size();
        }

        info!(
            "loaded {:?} tensors ({}B) ",
            model_contents.tensor_infos.len(),
            total_size_in_bytes,
        );

        ModelWeights::from_gguf(model_contents, cursor, &Device::Cpu)
    }

    fn try_load_ggml(cursor: &mut Cursor<Vec<u8>>) -> Result<ModelWeights, candle_core::Error> {
        info!("trying to load model assuming ggml");
        let model_contents = ggml_file::Content::read(cursor, &Device::Cpu)?;
        let mut total_size_in_bytes = 0;
        for (_, tensor) in model_contents.tensors.iter() {
            let elem_count = tensor.shape().elem_count();
            total_size_in_bytes +=
                elem_count * tensor.dtype().type_size() / tensor.dtype().block_size();
        }

        info!(
            "loaded {:?} tensors ({}B) ",
            model_contents.tensors.len(),
            total_size_in_bytes,
        );

        ModelWeights::from_ggml(model_contents, 1)
    }

    fn try_load_non_quantized(cursor: &mut Cursor<Vec<u8>>) -> Result<Model, candle_core::Error> {
        let config = llama2_c::Config::from_reader(cursor)?;
        println!("{config:?}");
        let weights =
            llama2_c_weights::TransformerWeights::from_reader(cursor, &config, &Device::Cpu)?;
        let vb = weights.var_builder(&config, &Device::Cpu)?;
        let cache = llama2_c::Cache::new(true, &config, vb.pp("rot"))?;
        let llama = Llama::load(vb, config.clone())?;
        Ok(Model::Llama {
            model: llama,
            cache,
        })
    }

    fn load_model(&self, model_weights: Vec<u8>) -> Model {
        let mut cursor = Cursor::new(model_weights);
        if let Ok(model) = Self::try_load_gguf(&mut cursor) {
            return Model::Qllama(model);
        }
        cursor.seek(SeekFrom::Start(0)).expect("seeking to 0");
        if let Ok(model) = Self::try_load_ggml(&mut cursor) {
            return Model::Qllama(model);
        }
        cursor.seek(SeekFrom::Start(0)).expect("seeking to 0");
        if let Ok(model) = Self::try_load_non_quantized(&mut cursor) {
            return model;
        }
        // might need a 'model not supported variant'
        panic!("model failed to be loaded")
    }

    fn run_model(&self, prompt_string: &str) -> Result<String, candle_core::Error> {
        let raw_weights = &self.model;
        let tokenizer_bytes = &self.tokenizer;

        let mut output = String::new();
        let mut model = self.load_model(raw_weights.clone());

        let tokenizer = Tokenizer::from_bytes(tokenizer_bytes).expect("failed to create tokenizer");
        let mut logits_processor = LogitsProcessor::new(299792458, None, None);
        let mut index_pos = 0;

        let mut tokens = tokenizer
            .encode(prompt_string, true)
            .unwrap()
            .get_ids()
            .to_vec();
        let mut tokenizer = TokenOutputStream::new(tokenizer);
        for index in 0.. {
            let context_size = if index > 0 { 1 } else { tokens.len() };
            let ctxt = &tokens[tokens.len().saturating_sub(context_size)..];
            let input = Tensor::new(ctxt, &Device::Cpu)?.unsqueeze(0)?;
            if index_pos == 256 {
                return Ok(output
                    .rsplit_once('.')
                    .map(|(before, _)| format!("{}.", before))
                    .unwrap_or_else(|| output.to_string()));
            }
            let logits = model.forward(&input, index_pos)?;
            let logits = logits.i((0, logits.dim(1)? - 1))?;

            let start_at = tokens.len().saturating_sub(10);
            candle_transformers::utils::apply_repeat_penalty(&logits, 0.5, &tokens[start_at..])?;
            index_pos += ctxt.len();

            let next_token = logits_processor.sample(&logits)?;
            tokens.push(next_token);
            if let Some(t) = tokenizer.next_token(next_token)? {
                output.push_str(&t);
            }
        }
        if let Some(rest) = tokenizer.decode_rest().unwrap() {
            output.push_str(&rest);
            output.insert_str(0, prompt_string);
        }
        Ok(output)
    }
}
