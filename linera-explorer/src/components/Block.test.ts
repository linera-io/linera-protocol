import { mount } from '@vue/test-utils'
import Block from './Block.vue'

test('Block mounting', () => {
  mount(Block, {
    props: {
      title: 'Block',
      block: {
        hash: "1fe0d0bb557f1a9057a2fca119566b439aa70d04918b71ea1485d5da2c7566b5",
        value: {
          status: "confirmed",
          block: {
            header: {
              version: 1,
              chainId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65",
              epoch: "0",
              height: 6,
              timestamp: 1694097511817833,
              authenticatedSigner: "a36c72207a7c3cef20eb254978c0947d7cf28c9c7d7c62de42a0ed9db901cf3f",
              previousBlockHash: "f1c748c5e39591125250e85d57fdeac0b7ba44a32c12c616eb4537f93b6e5d0a",
              stateHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
              messagesHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
              eventsHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
              bundlesHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
              operationsHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
              oracleResponsesHash: "5bcd40995283e74798c60e8dc7a93e8c61059440534070673dfb973b2b66f61a",
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
              events: [[]],
              oracleResponses: [],
              incomingBundles: [{
                origin: {
                  medium: "Direct",
                  sender: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
                },
                bundle: {
                  certificateHash: "f1c748c5e39591125250e85d57fdeac0b7ba44a32c12c616eb4537f93b6e5d0a",
                  height: 5,
                  messages: [{
                    authenticatedSigner: null,
                    message: { System: { BytecodePublished: { operation_index: 0 } } },
                    grant: "0.01",
                    index: 4,
                    kind: "Tracked"
                  }],
                  transactionIndex: 0,
                  timestamp: 1694097510206912
                },
                action: "Accept",
              }],
              operations: []
            }
          }
        }
      }
    },
  })
})
