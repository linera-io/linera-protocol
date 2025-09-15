[**@linera/client**](../README.md)

***

[@linera/client](../README.md) / Client

# Class: Client

Defined in: linera\_web.d.ts:68

The full client API, exposed to the wallet implementation. Calls
to this API can be trusted to have originated from the user's
request. This struct is the backend for the extension itself
(side panel, option page, et cetera).

## Constructors

### Constructor

> **new Client**(`wallet`, `signer`): `Client`

Defined in: linera\_web.d.ts:77

Creates a new client and connects to the network.

# Errors
On transport or protocol error, or if persistent storage is
unavailable.

#### Parameters

##### wallet

[`Wallet`](Wallet.md)

##### signer

[`Signer`](../interfaces/Signer.md)

#### Returns

`Client`

## Methods

### frontend()

> **frontend**(): [`Frontend`](Frontend.md)

Defined in: linera\_web.d.ts:109

Gets an object implementing the API for Web frontends.

#### Returns

[`Frontend`](Frontend.md)

***

### identity()

> **identity**(): `Promise`\<`any`\>

Defined in: linera\_web.d.ts:105

Gets the identity of the default chain.

# Errors
If the chain couldn't be established.

#### Returns

`Promise`\<`any`\>

***

### onNotification()

> **onNotification**(`handler`): `void`

Defined in: linera\_web.d.ts:86

Sets a callback to be called when a notification is received
from the network.

# Panics
If the handler function fails or we fail to subscribe to the
notification stream.

#### Parameters

##### handler

`Function`

#### Returns

`void`

***

### transfer()

> **transfer**(`options`): `Promise`\<`void`\>

Defined in: linera\_web.d.ts:98

Transfers funds from one account to another.

`options` should be an options object of the form `{ donor,
recipient, amount }`; omitting `donor` will cause the funds to
come from the chain balance.

# Errors
- if the options object is of the wrong form
- if the transfer fails

#### Parameters

##### options

`any`

#### Returns

`Promise`\<`void`\>
