# LLM Example Application

This example application runs a large language model in an application's service.

The model used by Linera Stories is a 40M parameter TinyLlama by A. Karpathy. Find out more here:
<https://github.com/karpathy/llama2.c>.

CAVEAT:

* Running larger LLMs with acceptable performance will likely require hardware acceleration ([#1931](https://github.com/linera-io/linera-protocol/issues/1931)).

* The service currently is restarted when the wallet receives a new block for the chain where the
  application is running from. That means it fetches the model again, which is inefficient. The
  service should be allowed to continue executing in that case
  ([#2160](https://github.com/linera-io/linera-protocol/issues/2160)).


## How It Works

Models and tokenizers are served locally using a local Python server. They are expected
at `model.bin` and `tokenizer.json`.

The application's service exposes a single GraphQL field called `prompt` which takes a prompt
as input and returns a response.

When the first prompt is submitted, the application's service uses the `fetch_url`
system API to fetch the model and tokenizer. Subsequently, the model bytes are converted
to the GGUF format where it can be used for inference.

## Usage

We're assuming that a local wallet is set up and connected to a running test network
(local or otherwise).

If you use `linera net up`, the value of the default chain ID is:
```bash
CHAIN=e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
```

See the file `linera-protocol/examples/fungible/README.md` or the [online developer
manual](https://linera.dev) for instructions.

### Using the LLM Application

First, deploy the application:
```bash
cd examples
APP_ID=$(linera project publish-and-create llm)
```

Then, a node service for the current wallet has to be started:

```bash
PORT=8080
linera --long-lived-services service --port $PORT &
 ```

The experimental option `--long-lived-services` is used for performance, to avoid
reloading the model between queries.

Next, navigate to `llm/web-frontend` and install the requisite `npm`
dependencies:

```bash
cd llm/web-frontend
npm install --no-save
BROWSER=none npm start
```

Finally, navigate to `localhost:3000` to interact with the Linera ChatBot.
```bash
echo "http://localhost:3000/$CHAIN?app=$APP_ID&port=$PORT"
```
