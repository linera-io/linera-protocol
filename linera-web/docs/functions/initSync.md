[**@linera/client**](../README.md)

***

[@linera/client](../README.md) / initSync

# Function: initSync()

> **initSync**(`module`, `memory?`): `InitOutput`

Defined in: linera\_web.d.ts:257

Instantiates the given `module`, which can either be bytes or
a precompiled `WebAssembly.Module`.

## Parameters

### module

Passing `SyncInitInput` directly is deprecated.

`SyncInitInput` | \{ `memory?`: `Memory`; `module`: `SyncInitInput`; `thread_stack_size?`: `number`; \}

### memory?

`Memory`

Deprecated.

## Returns

`InitOutput`
