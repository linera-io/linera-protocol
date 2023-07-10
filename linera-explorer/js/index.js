const rust = import("../pkg/index.js")

import template from '../html/app.html'
import json from './json.js'
import op_component from './operation.js'
import block_component from './block.js'
import blocks_component from './blocks.js'
import applications_component from './applications.js'
import application_component from './application.js'
import entrypoint_component from './entrypoint.js'
import input_type_component from './input_type.js'
import output_type_component from './output_type.js'

function main(r) {
  const app = Vue.createApp({
    template,
    data() { return r.data() },
    methods : {
      route(path, refresh) { r.route(this, path, refresh) },
      save_config() { r.save_config(this) },
    },
  })
  app.component('v-json', json.component)
  app.component('v-op', op_component)
  app.component('v-block', block_component)
  app.component('v-blocks', blocks_component)
  app.component('v-applications', applications_component)
  app.component('v-application', application_component)
  app.component('v-entrypoint', entrypoint_component(r))
  app.component('v-input-type', input_type_component(r))
  app.component('v-output-type', output_type_component(r))
  app.config.globalProperties.sh = r.short_cryptohash
  app.config.globalProperties.shapp = r.short_app_id
  app.config.globalProperties.json_load = json.load
  r.init(app.mount("#app"), window.location.href)
}

rust.then(r => main(r)).catch(console.error)
