[**@linera/client**](../README.md)

***

[@linera/client](../README.md) / default

# Function: default()

> **default**(`module_or_path?`, `memory?`): `Promise`\<`InitOutput`\>

Defined in: linera\_web.d.ts:268

If `module_or_path` is {RequestInfo} or {URL}, makes a request and
for everything else, calls `WebAssembly.instantiate` directly.

## Parameters

### module\_or\_path?

Passing `InitInput` directly is deprecated.

`InitInput` | `Promise`\<`InitInput`\> | \{ `memory?`: `Memory`; `module_or_path`: InitInput \| Promise\<InitInput\>; `thread_stack_size?`: `number`; \}

### memory?

`Memory`

Deprecated.

## Returns

`Promise`\<`InitOutput`\>
