import { mount } from '@vue/test-utils'
import Json from './Json.vue'

test('Json mounting', () => {
  mount(Json, {
    props: { data: { foo: 42, bar: 'foo' } },
  })
})
