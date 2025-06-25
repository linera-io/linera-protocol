[**@linera/client**](../README.md)

***

[@linera/client](../README.md) / Frontend

# Class: Frontend

Defined in: linera\_web.d.ts:141

The subset of the client API that should be exposed to application
frontends. Any function exported here with `wasm_bindgen` can be
called by untrusted Web pages, and so inputs must be verified and
outputs must not leak sensitive information without user
confirmation.

## Methods

### application()

> **application**(`id`): `Promise`\<`Application`\>

Defined in: linera\_web.d.ts:160

Retrieves an application for querying.

# Errors
If the application ID is invalid.

#### Parameters

##### id

`string`

#### Returns

`Promise`\<`Application`\>

***

### validatorVersionInfo()

> **validatorVersionInfo**(): `Promise`\<`any`\>

Defined in: linera\_web.d.ts:153

Gets the version information of the validators of the current network.

# Errors
If a validator is unreachable.

# Panics
If no default chain is set for the current wallet.

#### Returns

`Promise`\<`any`\>
