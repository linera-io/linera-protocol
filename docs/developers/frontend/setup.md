# Setting up the Frontend Development Environment

## Supported browsers

The Linera client library is supported by most mainstream browsers at the time
of writing (Baseline 2023). It does make use of some fairly modern features, so
if your browser version is too old you may struggle to follow this tutorial.
Specifically, your browser should support:

- [import maps](https://caniuse.com/import-maps)
- [`SharedArrayBuffer`](https://caniuse.com/sharedarraybuffer)
- [WebAssembly threading primitives](https://caniuse.com/wasm-threads)
- [top-level `await`](https://caniuse.com/mdn-javascript_operators_await_top_level)
  — this will be used for brevity in the tutorial, but is easy to factor out if
  your browser doesn't support it

## Creating a basic HTML page

Let's start by creating a simple HTML UI. This page won't connect to Linera yet,
but we can use it as scaffolding to get our development environment set up.
We'll call this page `index.html`, and it will be the only file we need to edit
to build our frontend.

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <title>Counter</title>
  </head>
  <body>
    <p>Chain: <span id="chain-id">requesting chain…</span></p>
    <p>Clicks: <span id="count">0</span></p>
    <button id="increment">Click me!</button>
  </body>
</html>
```

## Serving your frontend

In order to use the JavaScript client API, we will need a Web server. Since we
use a
[`SharedArrayBuffer`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/SharedArrayBuffer)
to share memory between WebAssembly threads, running your frontend from disk
using a `file://` URI will not work, as `SharedArrayBuffer` requires
[cross-origin isolation](https://developer.mozilla.org/en-US/docs/Web/API/Window/crossOriginIsolated)
for security.

In this tutorial we'll be using
[`http-server`](https://github.com/http-party/http-server), but any server will
do so long as it can set the `Cross-Origin-Opener-Policy` and
`Cross-Origin-Embedder-Policy` headers.

To use `http-server`, first ensure you have Node.js installed. On Ubuntu, this
can be accomplished with:

```shellsession
sudo apt install nodejs
```

Then, the command

```shellsession
npx http-party/http-server \
  --header Cross-Origin-Embedder-Policy:require-corp \
  --header Cross-Origin-Opener-Policy:same-origin
```

can be used to serve our HTML page on `localhost`.

```admonish info
Note that we use `http-party/http-server` here to use `http-server`
from GitHub.  Writing just `http-server` will pull the version from
npm, which at the time of writing is very old and doesn't support
custom headers.
```

## Getting the client library

The entire Linera client, WebAssembly and all, is published to the Node package
repository as [`@linera/client`](https://www.npmjs.com/package/@linera/client).
We'll include it into our `node_modules` with:

```shellsession
npm install @linera/client@{{#include ../../RELEASE_VERSION}}
```

````admonish warning title="A note on bundlers"
We're serving our `node_modules` here, so no bundling step is
required.  However, if you do choose to bundle your frontend, it is
important that both the Web worker entry point and the
`@linera/client` library itself remain in separate files, with their
signatures intact, in order for the Web worker to be able to refer to
them.  For example, if using Vite, make sure to define an extra
entrypoint for `@linera/client`, preserve its signature, and exclude
it from dependency optimization:

``` typescript
export default defineConfig({
  build: {
    rollupOptions: {
      input: {
        index: 'index.html',
        linera: '@linera/client',
      },
      preserveEntrySignatures: 'strict',
    },
  },
  optimizeDeps: {
    exclude: [
      '@linera/client',
    ],
  },
})
```
````
