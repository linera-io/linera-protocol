import { mount } from '@vue/test-utils'
import Op from './Op.vue'

test('Op mounting', () => {
  mount(Op, {
    props: {
      id: 'op',
      op: {
        System: {
          PublishModule: {
            contract: {
              bytes:"0061..7874"
            },
            service: {
              bytes:"0061..7874"
            }
          }
        }
      }
    },
  })
})
