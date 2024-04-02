# Llm Linera Example

TODO: this needs to be moved to `cargo doc`.

### Download Model and tokenizer

Small model: https://huggingface.co/TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF/tree/main

```bash
wget -c https://huggingface.co/spaces/TheBloke/Llama-2-7B-GGML/resolve/main/llama-2-7b.ggmlv3.q4_0.bin
wget -c https://huggingface.co/spaces/lmz/candle-llama2/resolve/main/tokenizer.json

wget -c https://huggingface.co/spaces/EleutherAI/gpt-neo-2.7B/resolve/main/gpt-neo-2.7B.bin
```

### Serve models locally

```bash
python3 -m http.server 10001
```