[**@linera/client**](../README.md)

***

[@linera/client](../README.md) / Signer

# Interface: Signer

Defined in: linera\_web.d.ts:26

Interface for signing and key management compatible with Ethereum (EVM) addresses.

## Methods

### containsKey()

> **containsKey**(`owner`): `Promise`\<`boolean`\>

Defined in: linera\_web.d.ts:43

Checks whether the instance holds a key whose associated address matches the given EVM address.

#### Parameters

##### owner

`string`

The EVM address to check for.

#### Returns

`Promise`\<`boolean`\>

A promise that resolves to `true` if the key exists and matches the given address, otherwise `false`.

***

### sign()

> **sign**(`owner`, `value`): `Promise`\<`string`\>

Defined in: linera\_web.d.ts:35

Signs a given value using the private key associated with the specified EVM address.
The signing process must follow the EIP-191 standard.

#### Parameters

##### owner

`string`

The EVM address whose private key will be used to sign the value.

##### value

`Uint8Array`

The data to be signed, as a `Uint8Array`.

#### Returns

`Promise`\<`string`\>

A promise that resolves to the EIP-191-compatible signature in hexadecimal string format.
