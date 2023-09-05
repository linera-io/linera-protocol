import { set_test_config } from './utils'
import { mount } from '@vue/test-utils'
import Entrypoint from './Entrypoint.vue'

test('Entrypoint mounting', () => {
  set_test_config().then(() => {
    mount(Entrypoint, {
      props: {
        kind: 'query',
        link: 'http://localhost:8080/chains/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65020000000000000000000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65040000000000000000000000',
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
