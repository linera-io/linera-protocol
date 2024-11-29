// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod random;
mod token;

use std::{
    io::{Cursor, Seek, SeekFrom},
    sync::Arc,
};

use async_graphql::{Context, EmptyMutation, EmptySubscription, Object, Request, Response, Schema};
use candle_core::{
    quantized::{ggml_file, gguf_file},
    Device, IndexOp, Tensor,
};
use candle_transformers::{
    generation::LogitsProcessor,
    models::{llama2_c, llama2_c::Llama, llama2_c_weights, quantized_llama::ModelWeights},
};
use linera_sdk::{base::WithServiceAbi, Service, ServiceRuntime};
use log::{debug, info};
use sha3::{Digest as _, Sha3_256};
use tokenizers::Tokenizer;

use crate::token::TokenOutputStream;

/// The SHA3-256 hash of the model weights to use.
const WEIGHTS_HASH: &[u8] = &[
    0x23, 0x42, 0x71, 0xe1, 0xf8, 0x3b, 0x6e, 0xec, 0xf1, 0x9b, 0xa4, 0xb7, 0xf4, 0x52, 0x49, 0xe7,
    0xd9, 0xc6, 0x86, 0x57, 0xbc, 0xa0, 0x2d, 0xa3, 0x9b, 0xdb, 0xb1, 0x49, 0xcd, 0x53, 0x10, 0x01,
];

/// The SHA3-256 hash of the tokenizer to use.
const TOKENIZER_HASH: &[u8] = &[
    0x4a, 0x33, 0x4c, 0x71, 0x85, 0x96, 0xca, 0x0b, 0x0a, 0x03, 0x11, 0x56, 0x0a, 0x50, 0x25, 0xfd,
    0xfc, 0x36, 0x8f, 0x33, 0x64, 0x17, 0x2b, 0x74, 0x01, 0xbb, 0x89, 0xbf, 0x30, 0x99, 0x20, 0x0b,
];

pub struct LlmService {
    model_context: Arc<ModelContext>,
}

linera_sdk::service!(LlmService);

impl WithServiceAbi for LlmService {
    type Abi = llm::LlmAbi;
}

struct QueryRoot {}

#[Object]
impl QueryRoot {
    async fn prompt(&self, ctx: &Context<'_>, prompt: String) -> String {
        let model_context = ctx.data::<Arc<ModelContext>>().unwrap();
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
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        info!("Downloading model");
        let raw_weights = runtime
            .fetch_url("https://huggingface.co/karpathy/tinyllamas/resolve/main/stories42M.bin");
        assert_eq!(
            Sha3_256::digest(&raw_weights).as_slice(),
            WEIGHTS_HASH,
            "Incorrect model was fetched"
        );
        info!("Downloaded model weights: {} bytes", raw_weights.len());

        info!("Downloading tokenizer");
        let tokenizer_bytes = runtime.fetch_url(
            "https://huggingface.co/spaces/lmz/candle-llama2/resolve/main/tokenizer.json",
        );
        assert_eq!(
            Sha3_256::digest(&tokenizer_bytes).as_slice(),
            TOKENIZER_HASH,
            "Incorrect tokenizer was fetched"
        );
        info!("Downloaded tokenizer: {} bytes", tokenizer_bytes.len());

        let model_context = Arc::new(ModelContext {
            model: raw_weights,
            tokenizer: tokenizer_bytes,
        });
        LlmService { model_context }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let query_string = &request.query;
        debug!("query: {}", query_string);
        let schema = Schema::build(QueryRoot {}, EmptyMutation, EmptySubscription)
            .data(self.model_context.clone())
            .finish();
        schema.execute(request).await
    }
}

impl ModelContext {
    fn try_load_gguf(cursor: &mut Cursor<Vec<u8>>) -> Result<ModelWeights, candle_core::Error> {
        debug!("trying to load model assuming gguf");
        let model_contents = gguf_file::Content::read(cursor)?;
        let mut total_size_in_bytes = 0;
        for (_, tensor) in model_contents.tensor_infos.iter() {
            let elem_count = tensor.shape.elem_count();
            total_size_in_bytes +=
                elem_count * tensor.ggml_dtype.type_size() / tensor.ggml_dtype.block_size();
        }

        debug!(
            "loaded {:?} tensors ({}B) ",
            model_contents.tensor_infos.len(),
            total_size_in_bytes,
        );

        ModelWeights::from_gguf(model_contents, cursor, &Device::Cpu)
    }

    fn try_load_ggml(cursor: &mut Cursor<Vec<u8>>) -> Result<ModelWeights, candle_core::Error> {
        debug!("trying to load model assuming ggml");
        let model_contents = ggml_file::Content::read(cursor, &Device::Cpu)?;
        let mut total_size_in_bytes = 0;
        for (_, tensor) in model_contents.tensors.iter() {
            let elem_count = tensor.shape().elem_count();
            total_size_in_bytes +=
                elem_count * tensor.dtype().type_size() / tensor.dtype().block_size();
        }

        debug!(
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
