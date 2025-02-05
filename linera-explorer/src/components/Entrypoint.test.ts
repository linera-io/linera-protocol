import { set_test_config } from './utils'
import { mount } from '@vue/test-utils'
import Entrypoint from './Entrypoint.vue'

test('Entrypoint mounting', () => {
  set_test_config().then(() => {
    mount(Entrypoint, {
      props: {
        kind: 'query',
        link: 'http://localhost:8080/chains/aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8/applications/aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8020000000000000000000000aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8040000000000000000000000',
        entry: {
          name: 'accounts',
          type: {
            kind: 'SCALAR',
            name: 'Amount',
            _include: true,
          },
          args: [ {
            name: 'accountOwner',
            type: {
              kind: 'SCALAR',
              name: 'AccountOwner'
            }
          } ],
        }
      },
    })
  })
})
