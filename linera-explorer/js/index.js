const rust = import("../pkg/index.js")

import json from './json.js'
import op_component from './operation.js'
import block_component from './block.js'
import template from '../html/app.html'

function main(r) {
  const app = Vue.createApp({
    template,
    data() { return r.data() },
    methods : {
      route(path, refresh) { r.route(this, path, refresh) },
    },
  }).component('v-block', block_component).component('v-op', op_component).component('v-json', json.component)
  app.config.globalProperties.sh = r.short
  app.config.globalProperties.json_load = json.load
  r.init(app.mount("#app"))
}

rust.then(r => main(r)).catch(console.error)
