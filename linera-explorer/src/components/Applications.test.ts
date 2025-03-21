import { set_test_config } from './utils'
import { mount } from '@vue/test-utils'
import Applications from './Applications.vue'

test('Applications mounting', () => {
  set_test_config().then(() => {
    mount(Applications, {
      props: {
        apps: [ {
          id: "aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8020000000000000000000000aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8040000000000000000000000",
          link: "http://localhost:8080/chains/aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8/applications/aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8020000000000000000000000aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8040000000000000000000000",
          description: {
            module_id: "aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8020000000000000000000000",
            bytecode_location: {
              certificate_hash: "095f5b1ebb21eeb90fb760531789f85559efd721484bd0e333ec92270ad9bb38",
              operation_index: 0,
            },
            creation: {
              chain_id: "aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8",
              height: 4,
              index: 0,
            },
            parameters: [ 110, 117, 108, 108 ],
            required_applications: [],
          } } ]
      },
    })
  })
})
