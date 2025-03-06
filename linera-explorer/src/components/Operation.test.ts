import { set_test_config } from './utils'
import { mount } from '@vue/test-utils'
import Operation from './Operation.vue'

test('Operation mounting', () => {
  set_test_config().then(() => {
    mount(Operation, {
      props: {
        op: {
          key: {
            chain_id: "aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8",
            height: 5,
            index: 0
          },
          previousOperation: {
            chain_id: "aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8",
            height: 4, index: 0
          },
          index: 4,
          block: "f1c748c5e39591125250e85d57fdeac0b7ba44a32c12c616eb4537f93b6e5d0a",
          content: {
            System: {
              PublishModule: {
                contract: { bytes: "0061..7874" },
                service: { bytes: "0061..7874" }
              }
            }
          }
        }
      },
    })
  })
})
