import { set_test_config } from './utils'
import { mount } from '@vue/test-utils'
import App from './App.vue'

test('App mounting', () => {
  set_test_config().then(() => {
    mount(App)
  })
})
