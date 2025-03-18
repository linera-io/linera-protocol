import { set_test_config } from './utils'
import { mount } from '@vue/test-utils'
import InputType from './InputType.vue'

test('InputType mounting', () => {
  set_test_config().then(() => {
    mount(InputType, {
      props: {
        elt: {
          kind: 'SCALAR',
          name: 'Address'
        }, offset: false
      },
    })
  })
})
