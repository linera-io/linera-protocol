import { set_test_config } from './utils'
import { mount } from '@vue/test-utils'
import Applications from './Applications.vue'

test('Applications mounting', () => {
  set_test_config().then(() => {
    mount(Applications, {
      props: {
        apps: [ {
          id: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65020000000000000000000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65040000000000000000000000",
          link: "http://localhost:8080/chains/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65020000000000000000000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65040000000000000000000000",
          description: {
            bytecode_id: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65020000000000000000000000",
            bytecode_location: {
              certificate_hash: "095f5b1ebb21eeb90fb760531789f85559efd721484bd0e333ec92270ad9bb38",
              operation_index: 0,
            },
            creation: {
              chain_id: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65",
              height: 4,
              index: 0,
            },
            parameters: [ 110, 117, 108, 108 ],
            required_application_ids: [],
          } } ]
      },
    })
  })
})
