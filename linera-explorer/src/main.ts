import init, { start, short_crypto_hash, short_app_id } from "../pkg/linera_explorer"
import { createApp, ComponentPublicInstance } from 'vue'
import { json_load, operation_id } from './components/utils'
import { Scalars } from '../gql/operations'
import App from './components/App.vue'

import 'bootstrap/dist/css/bootstrap.min.css'
import 'bootstrap-icons/font/bootstrap-icons.css'
import 'bootstrap/dist/js/bootstrap.bundle.min.js'

declare module '@vue/runtime-core' {
  interface ComponentCustomProperties {
    shapp: (id: string) => string,
    sh: (hash: string) => string,
    json_load: (id: string, data: any) => void,
    operation_id: (key: Scalars['OperationKey']['output']) => string,
    $root: ComponentPublicInstance<{ route: (name?: string, args?: [string, string][]) => void }>,
  }
}

init().then(() => {
  const app = createApp(App)
  app.config.globalProperties.sh = short_crypto_hash
  app.config.globalProperties.shapp = short_app_id
  app.config.globalProperties.json_load = json_load
  app.config.globalProperties.operation_id = operation_id
  start(app.mount('#app'))
}).catch(console.error)
