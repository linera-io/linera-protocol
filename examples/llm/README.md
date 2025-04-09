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

Before getting started, make sure that the binary tools `linera*` corresponding to
your version of `linera-sdk` are in your PATH. For scripting purposes, we also assume
that the BASH function `linera_spawn` is defined.

From the root of Linera repository, this can be achieved as follows:

```bash
export PATH="$PWD/target/debug:$PATH"
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"
```

Next, start the local Linera network and run a faucet:

```bash
FAUCET_PORT=8079
FAUCET_URL=http://localhost:$FAUCET_PORT
linera_spawn linera net up --with-faucet --faucet-port $FAUCET_PORT

# If you're using a testnet, run this instead:
#   LINERA_TMP_DIR=$(mktemp -d)
#   FAUCET_URL=https://faucet.testnet-XXX.linera.net  # for some value XXX
```

Create the user wallet and add chains to it:

```bash
export LINERA_WALLET="$LINERA_TMP_DIR/wallet.json"
export LINERA_STORAGE="rocksdb:$LINERA_TMP_DIR/client.db"

linera wallet init --faucet $FAUCET_URL

INFO=($(linera wallet request-chain --faucet $FAUCET_URL))
CHAIN="${INFO[0]}"
OWNER="${INFO[3]}"
```

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

```bash,ignore
cd llm/web-frontend
npm install --no-save
BROWSER=none npm start
```

Finally, navigate to `localhost:3000` to interact with the Linera ChatBot.

```bash,ignore
echo "http://localhost:3000/$CHAIN?app=$APP_ID&port=$PORT"
```
