const rust = import("../pkg/index.js")

import json from './json.js'
import op_component from './operation.js'
import block_component from './block.js'
import blocks_component from './blocks.js'
import application_component from './application.js'
import applications_component from './applications.js'
import template from '../html/app.html'

function main(r) {
  const app = Vue.createApp({
    template,
    data() { return r.data() },
    methods : {
      route(path, refresh) { r.route(this, path, refresh) },
      save_config() { r.save_config(this) },
    },
  })
  app.component('v-blocks', blocks_component)
  app.component('v-application', application_component)
  app.component('v-applications', applications_component)
  app.component('v-block', block_component)
  app.component('v-op', op_component)
  app.component('v-json', json.component)
  app.config.globalProperties.sh = r.short
  app.config.globalProperties.shapp = r.short_app
  app.config.globalProperties.json_load = json.load
  r.init(app.mount("#app"))
}

rust.then(r => main(r)).catch(console.error)
