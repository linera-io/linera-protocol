const rust = import("../pkg/index.js")

import VueJsonPretty from 'vue-json-pretty'

function map_to_object(m) {
  if (typeof(m) == 'object') {
    if (Array.isArray(m)) {
      return m.reduce((acc, x) => { acc.push(map_to_object(x)); return acc }, [])
    } else {
      let m2 = (m instanceof Map) ? Object.fromEntries(m) : m
      return Object.keys(m2).reduce((acc, k) => { acc[k] = map_to_object(m2[k]); return acc }, {})
    }
  } else {
    return m
  }
}

function main(r) {
  let block_component = {
    template: "#block-template",
    props: ["block","title"]
  }

  const app = Vue.createApp({
    data() { return r.data() },
    methods : {
      test() { r.test(this) },
      get_blocks() { r.get_blocks(this) },
      route(path, refresh) { r.route(this, path, refresh) },
      sh(s) { return r.short(s) },
    },
  }).component('v-block', block_component).component('v-json', VueJsonPretty)
  app.config.globalProperties.mtoo = map_to_object
  r.init(app.mount("#app"))
}

rust.then(r => main(r)).catch(console.error)
