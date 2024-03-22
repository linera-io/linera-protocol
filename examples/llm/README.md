# Llm Linera Example

TODO: this needs to be moved to `cargo doc`.

### Download Model and tokenizer

```bash
wget -c https://huggingface.co/spaces/lmz/candle-llama2/resolve/main/model.bin
wget -c https://huggingface.co/spaces/lmz/candle-llama2/resolve/main/tokenizer.json
```

### Serve models locally

```bash
python3 -m http.server 10001
```