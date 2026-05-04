import { flushPromises, mount } from '@vue/test-utils'
import { set_test_config } from './utils'
import Block from './Block.vue'

// Build a minimal `ConfirmedBlock` shape with caller-provided `events`,
// `operationResults`, and `transactionMetadata`. Defaults to empty so each test
// can assert about exactly the slice it cares about.
function makeBlock(opts: {
  hash?: string,
  events?: any[][],
  operationResults?: any[],
  transactionMetadata?: any[]
} = {}) {
  const hash = opts.hash ?? 'decoded-block-hash'
  return {
    hash,
    status: 'confirmed',
    block: {
      header: {
        chainId: 'aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8',
        epoch: '0',
        height: 1,
        timestamp: 1694097511817833,
        authenticatedSigner: null,
        previousBlockHash: 'f1c748c5e39591125250e85d57fdeac0b7ba44a32c12c616eb4537f93b6e5d0a',
        stateHash: '5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a',
        transactionsHash: '5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a',
        messagesHash: '5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a',
        previousMessageBlocksHash: '5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a',
        previousEventBlocksHash: '5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a',
        eventsHash: '5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a',
        oracleResponsesHash: '5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a',
        blobsHash: '5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a',
        operationResultsHash: '5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a',
      },
      body: {
        messages: [[]],
        previousMessageBlocks: {},
        previousEventBlocks: {},
        events: opts.events ?? [[]],
        oracleResponses: [],
        blobs: [[]],
        operationResults: opts.operationResults ?? [],
        transactionMetadata: opts.transactionMetadata ?? []
      }
    }
  }
}

function mountBlockWithRoot(block: any, opts: {
  formats_registry?: string | null,
  decode_user_event_value?: (app: string, bytes: string) => any,
  decode_user_response?: (app: string, bytes: string) => any
} = {}) {
  return mount(Block, {
    props: { title: 'Block', block },
    global: {
      config: {
        globalProperties: {
          config: {
            formats_registry_chain: opts.formats_registry ? 'fake-chain-id' : null,
            formats_registry_app_id: opts.formats_registry ?? null,
          },
          decode_user_event_value: (application_id: string, bytes_hex: string) =>
            opts.decode_user_event_value ? opts.decode_user_event_value(application_id, bytes_hex) : null,
          decode_user_response: (application_id: string, bytes_hex: string) =>
            opts.decode_user_response ? opts.decode_user_response(application_id, bytes_hex) : null
        } as any
      }
    }
  })
}

test('decodes user event value when registry is configured', async () => {
  await set_test_config()
  const decode = vi.fn(async (_app: string, _bytes: string) => ({ counter: 7 }))
  const block = makeBlock({
    hash: 'evt-block',
    events: [[{
      index: 0,
      streamId: {
        applicationId: { User: 'app-evt-1' },
        streamName: [101, 118, 116]
      },
      value: [0xde, 0xad, 0xbe, 0xef]
    }]]
  })
  const wrapper = mountBlockWithRoot(block, {
    formats_registry: 'fake-registry-app-id',
    decode_user_event_value: decode
  })
  await flushPromises()
  expect(decode).toHaveBeenCalledWith('app-evt-1', 'deadbeef')
  expect(wrapper.text()).toContain('Decoded Event:')
  expect(wrapper.text()).toContain('counter')
  expect(wrapper.text()).toContain('7')
})

test('does not decode system event value', async () => {
  await set_test_config()
  const decode = vi.fn()
  const block = makeBlock({
    hash: 'evt-sys-block',
    events: [[{
      index: 0,
      streamId: {
        applicationId: 'System',
        streamName: [115, 121, 115]
      },
      value: [0x01, 0x02]
    }]]
  })
  mountBlockWithRoot(block, {
    formats_registry: 'fake-registry-app-id',
    decode_user_event_value: decode
  })
  await flushPromises()
  expect(decode).not.toHaveBeenCalled()
})

test('decodes user operation response when registry is configured', async () => {
  await set_test_config()
  const decode = vi.fn(async (_app: string, _bytes: string) => ({ ok: true, balance: '999' }))
  const block = makeBlock({
    hash: 'resp-block',
    transactionMetadata: [{
      transactionType: 'ExecuteOperation',
      operation: {
        operationType: 'User',
        applicationId: 'app-resp-1',
        userBytesHex: 'aa',
        systemOperation: null
      },
      incomingBundle: null
    }],
    operationResults: [[0xca, 0xfe]]
  })
  const wrapper = mountBlockWithRoot(block, {
    formats_registry: 'fake-registry-app-id',
    decode_user_response: decode
  })
  await flushPromises()
  expect(decode).toHaveBeenCalledWith('app-resp-1', 'cafe')
  expect(wrapper.text()).toContain('Decoded Response:')
  expect(wrapper.text()).toContain('balance')
  expect(wrapper.text()).toContain('999')
})

test('does not decode response for system operation', async () => {
  await set_test_config()
  const decode = vi.fn()
  const block = makeBlock({
    hash: 'resp-sys-block',
    transactionMetadata: [{
      transactionType: 'ExecuteOperation',
      operation: {
        operationType: 'System',
        applicationId: null,
        userBytesHex: null,
        systemOperation: { systemOperationType: 'CloseChain' }
      },
      incomingBundle: null
    }],
    operationResults: [[0x01]]
  })
  mountBlockWithRoot(block, {
    formats_registry: 'fake-registry-app-id',
    decode_user_response: decode
  })
  await flushPromises()
  expect(decode).not.toHaveBeenCalled()
})

test('Block mounting', async () => {
  await set_test_config()
  mount(Block, {
    props: {
      title: 'Block',
      block: {
        hash: "1fe0d0bb557f1a9057a2fca119566b439aa70d04918b71ea1485d5da2c7566b5",
        status: "confirmed",
        block: {
          header: {
            chainId: "aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8",
            epoch: "0",
            height: 6,
            timestamp: 1694097511817833,
            authenticatedSigner: "a36c72207a7c3cef20eb254978c0947d7cf28c9c7d7c62de42a0ed9db901cf3f",
            previousBlockHash: "f1c748c5e39591125250e85d57fdeac0b7ba44a32c12c616eb4537f93b6e5d0a",
            stateHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
            transactionsHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
            messagesHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
            previousMessageBlocksHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
            previousEventBlocksHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
            eventsHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
            oracleResponsesHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
            blobsHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
            operationResultsHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
          },
          body: {
            messages: [[{
              destination: { Subscribers: [1] },
              authenticatedSigner: null,
              kind: "Protected",
              grant: 0,
              message: {
                System: {
                  BytecodeLocations: {
                    locations: [
                      [
                        "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65020000000000000000000000",
                        { certificateHash: "a4167c67ce9c94c301fd5cbbefeccf6c8e56d568a4c75ed85e93bfacee66bac5", operation_index: 0 }],
                      [
                        "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65050000000000000000000000",
                        { certificateHash: "f1c748c5e39591125250e85d57fdeac0b7ba44a32c12c616eb4537f93b6e5d0a", operation_index: 0 }]]
                  }
                }
              }
            }]],
            previousMessageBlocks: {},
            previousEventBlocks: {},
            events: [[]],
            oracleResponses: [],
            blobs: [[]],
            operationResults: [],
            transactionMetadata: [{
              transactionType: "ReceiveMessages",
              incomingBundle: {
                origin: {
                  medium: "Direct",
                  sender: "aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8"
                },
                bundle: {
                  certificateHash: "f1c748c5e39591125250e85d57fdeac0b7ba44a32c12c616eb4537f93b6e5d0a",
                  height: 5,
                  messages: [{
                    authenticatedSigner: null,
                    message: { System: { BytecodePublished: { operation_index: 0 } } },
                    grant: "0.01",
                    index: 4,
                    kind: "Tracked",
                    messageMetadata: {
                      messageType: "System",
                      applicationId: null,
                      userBytesHex: null,
                      systemMessage: null
                    }
                  }],
                  transactionIndex: 0,
                  timestamp: 1694097510206912
                },
                action: "Accept",
              },
              operation: null
            }]
          }
        }
      }
    },
  })
})
