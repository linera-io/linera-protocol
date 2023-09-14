import { mount } from '@vue/test-utils'
import OutputType from './OutputType.vue'

test('OutputType mounting', () => {
  mount(OutputType, {
    props: {
      elt: {
        kind: 'SCALAR',
        name: 'Amount',
        _include: true,
      },
      name: 'accountOwner',
      depth: 0,
    },
  })
})
