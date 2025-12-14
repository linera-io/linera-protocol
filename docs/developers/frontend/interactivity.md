# Interacting with Linera

Add a `<script type="module">` to your page. The location of this module doesn't
matter: module scripts are deferred until after page load. This is where we will
write all the JavaScript necessary to interact with Linera.

## Importing the Linera client library

To add the Linera client library to your page, put the following import map into
the `<head>` of your HTML:

```html
<script type="importmap">
  {
    "imports": {
      "@linera/client": "./node_modules/@linera/client/dist/linera_web.js"
    }
  }
</script>
```

Now the module `@linera/client` is available for import in your module:

```html
<script type="module">
  import * as linera from '@linera/client';
</script>
```

## Referring to the counter app

We'll need the application ID of the counter app deployed on our network of
choice. This tutorial uses Testnet, and the following application ID refers to a
counter app published there:

```javascript
const COUNTER_APP_ID =
  '2b1a0df8868206a4b7d6c2fdda911e4355d6c0115b896d4947ef8e535ee3c6b8';
```

If you wish to use a different network or deploy your own backend, you may need
to change the application ID. So long as it points to an application satisfying
the counter ABI, the rest of this tutorial will work without modification.

## Initialization

The first thing we need to do to interact with the Linera client library is
initialize it. This will download the WebAssembly binary, create a new memory
for it, and initialize the memory.

```javascript
await linera.default();
```

## Getting a wallet

If you have a wallet file available, you can use the `linera.Wallet.fromJson`
function to create a `linera.Wallet` from it. However, for the purposes of the
tutorial, we will connect to the Testnet faucet and create a new wallet with a
fresh chain owning some tokens. We will also update our `#chain-id` element to
let the user know the ID of their new chain.

```javascript
const faucet = await new linera.Faucet(
  'https://faucet.{{#include ../../RELEASE_DOMAIN}}.linera.net',
);
const wallet = await faucet.createWallet();
const client = await new linera.Client(wallet);
document.getElementById('chain-id').innerText = await faucet.claimChain(client);
```

## Communicating with the application

Calling the method `client.application(applicationId)` will get you an object
representing the application backend.

```javascript
const backend = await client.frontend().application(COUNTER_APP_ID);
```

You can query the backend application using the `query` method, which takes an
arbitrary string that will be passed to the backend as a request, and returns a
`Promise` of the response. We can use this to update our `#count` element with
the current value of the counter.

```javascript
async function updateCount() {
  const response = await backend.query('{ "query": "query { value }" }');
  document.getElementById('count').innerText = JSON.parse(response).data.value;
}

updateCount();
```

The counter application uses GraphQL as its request language. By convention,
Linera applications accept GraphQL as strings of JSON in the
[Apollo Server POST format](https://www.apollographql.com/docs/apollo-server/v2/requests),
but your application is free to accept whatever format it wants.

GraphQL `query` operations can be used to inspect the state of the application,
while `mutation` operations cause the client to propose new blocks with the
result of the requested modification. Let's attach an event handler to our
button that proposes an increment to the counter value.

```javascript
document.getElementById('increment').addEventListener('click', () => {
  backend.query('{ "query": "mutation { increment(value: 1) }" }');
});
```

## Notifications and reactivity

If you click the button, the value of the counter will go up, but the UI element
currently won't change to reflect it. Let's fix that.

The `Client` object also supports adding a callback for notifications. This is
key to Linera's reactivity: if something happens to one of a client's chains,
this callback will immediately be called with a notification object containing
information about the event.

In this case the only updates we're interested in are new blocks, which imply
that the counter value has changed, so whenever we see a new block let's update
the counter.

```javascript
client.onNotification(notification => {
  if (notification.reason.NewBlock) updateCount();
});
```

We're the sole owner of this chain, so the value query is purely local: nobody
could have updated the chain state but us. In the general case, though, other
users on other clients could update the chain with new blocks or by sending it
messages, and we'd get immediately notified in just the same way.

## Conclusion

That's it! In a few lines of code we've implemented an application frontend that
communicates with the Linera testnet and allows bidirectional communication with
an application, including realtime updates when the chain state changes.

A somewhat fleshed-out version of the code from this tutorial can be found in
the `examples/hosted-counter` subdirectory of the
[`linera-web` repository](https://github.com/linera-io/linera-web), next to some
more complicated examples. Alternatively, you can
[try it out online](https://demos.linera.net/hosted/counter).
