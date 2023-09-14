import { set_test_config } from './utils'
import { mount } from '@vue/test-utils'
import Plugin from './Plugin.vue'

test('Plugin mounting', () => {
  set_test_config().then(() => {
    mount(Plugin, {
      props: {
        plugin: {
          name: "operations",
          link: "http://localhost:8081/operations",
          queries: [],
        }
      },
    })
  })
})
