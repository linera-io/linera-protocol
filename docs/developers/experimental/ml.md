# Machine Learning on Linera

The Linera application contract / service split allows for securely and
efficiently running machine learning models on the edge.

The application's contract retrieves the correct model with all the correctness
guarantees enforced by the consensus algorithm, while the client performs
inference off-chain, in the un-metered service. Since the service is running on
the user's own hardware, it can be implicitly trusted.

## Guidelines

The existing examples use the [`candle`](https://github.com/huggingface/candle)
framework by [Hugging Face](https://huggingface.co/) as the underlying ML
framework.

`candle` is a minimalist ML framework for Rust with a focus on performance and
usability. It also compiles to Wasm and has great support for Wasm both in and
outside the browser. Check candle's
[examples](https://github.com/huggingface/candle/tree/main/candle-wasm-examples)
for inspiration on the types of models which are supported.

### Getting started

To add ML capabilities to your existing Linera project, you'll need to add the
`candle-core`, `getrandom`, `rand` and `tokenizers` dependencies to your Linera
project:

```toml
candle-core = "0.4.1"
getrandom = { version = "0.2.12", default-features = false, features = ["custom"] }
rand = "0.8.5"
```

Optionally, to run Large Language Models, you'll also need the
`candle-transformers` and `transformers` crate:

```toml
candle-transformers = "0.4.1"
tokenizers = { git = "https://github.com/christos-h/tokenizers", default-features = false, features = ["unstable_wasm"] }
```

### Providing randomness

ML frameworks use random numbers to perform inference. Linera services run in a
Wasm VM which does not have access to the OS Rng. For this reason, we need to
manually seed RNG used by `candle`. We do this by writing a custom `getrandom`.

Create a file under `src/random.rs` and add the following:

```rust,ignore
use std::sync::{Mutex, OnceLock};

use rand::{rngs::StdRng, Rng, SeedableRng};

static RNG: OnceLock<Mutex<StdRng>> = OnceLock::new();

fn custom_getrandom(buf: &mut [u8]) -> Result<(), getrandom::Error> {
    let seed = [0u8; 32];
    RNG.get_or_init(|| Mutex::new(StdRng::from_seed(seed)))
        .lock()
        .expect("failed to get RNG lock")
        .fill(buf);
    Ok(())
}

getrandom::register_custom_getrandom!(custom_getrandom);
```

This will enable `candle` and any other crates which rely on `getrandom` access
to a deterministic RNG. If deterministic behaviour is not desired, the System
API can be used to seed the RNG from a timestamp.

### Loading the model into the service

Models cannot currently be saved on-chain; for more information see the
`Limitations` below.

To perform model inference, the model must be loaded into the service. To do
this we'll use the `fetch_url` API when a query is made against the service:

```rust,ignore
impl Service for MyService {
    async fn handle_query(&self, request: Request) -> Response {
        // do some stuff here
        let raw_weights = self.runtime.fetch_url("https://my-model-provider.com/model.bin");
        // do more stuff here
    }
}
```

This can be served from a local webserver or pulled directly from a model
provider such as Hugging Face.

At this point we have the raw bytes which correspond to the models and
tokenizer. `candle` supports multiple formats for storing model weights, both
quantized and not (`gguf`, `ggml`, `safetensors`, etc.).

Depending on the model format that you're using, `candle` exposes convenience
functions to convert the bytes into a typed `struct` which can then be used to
perform inference. Below is an example for a non-quantized Llama 2 model:

```rust,ignore
    fn load_llama_model(cursor: &mut Cursor<Vec<u8>>) -> Result<(Llama, Cache), candle_core::Error> {
        let config = llama2_c::Config::from_reader(cursor)?;
        let weights =
            llama2_c_weights::TransformerWeights::from_reader(cursor, &config, &Device::Cpu)?;
        let vb = weights.var_builder(&config, &Device::Cpu)?;
        let cache = llama2_c::Cache::new(true, &config, vb.pp("rot"))?;
        let llama = Llama::load(vb, config.clone())?;
        Ok((llama, cache))
    }
```

### Inference

Performing inference using `candle` is not a 'one-size-fits-all' process.
Different models require different logic to perform inference so the specifics
of how to perform inference are beyond the scope of this document.

Luckily, there are multiple examples which can be used as guidelines on how to
perform inference in Wasm:

- [Llm Stories](https://github.com/linera-io/linera-protocol/tree/main/examples/llm)
- [Generative NFTs](https://github.com/linera-io/linera-protocol/tree/main/examples/gen-nft)
- [Candle Wasm Examples](https://github.com/huggingface/candle/tree/main/candle-wasm-examples)

## Limitations

### Hardware acceleration

Although SIMD instructions _are_ supported by the service runtime, general
purpose GPU hardware acceleration is
[not currently supported](https://github.com/linera-io/linera-protocol/issues/1931).
Therefore, performance in local model inference is degraded for larger models.

### On-chain models

Due to block-size constraints, models need to be stored off-chain until the
introduction of the
[Blob API](https://github.com/linera-io/linera-protocol/issues/1981). The Blob
API will enable large binary blobs to be stored on-chain, the correctness and
availability of which is guaranteed by the validators.

### Maximum model size

The maximum size of a model which can be loaded into an application's service is
currently constrained by:

1. The addressable memory of the service's Wasm runtime being 4 GiB.
2. Not being able to load models directly to the GPU.

It is recommended that smaller models (50 MB - 100 MB) are used at current state
of development.
