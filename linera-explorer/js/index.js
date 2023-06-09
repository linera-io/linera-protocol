const rust = import("../pkg/index.js")

import JSONFormatter from 'json-formatter-js'

function main(r) {
  let block_component = {
    template: "#block-template",
    props: ["block","title", "route"],
  }

  let json_component = {
    template: '<div :id="inner_id"></div>',
    props: ["id", "data"],
    data() { return { inner_id: this.id + '-json' } },
    mounted() {
      let formatter = new JSONFormatter(this.data, Infinity)
      let elt = document.getElementById(this.inner_id)
      elt.appendChild(formatter.render())
    }
  }

  const app = Vue.createApp({
    data() { return r.data() },
    methods : {
      test() { r.test(this) },
      get_blocks() { r.get_blocks(this) },
      route(path, refresh) { r.route(this, path, refresh) },
      sh(s) { return r.short(s) },
    },
  }).component('v-block', block_component).component('v-json', json_component)
  r.init(app.mount("#app"))
}

rust.then(r => main(r)).catch(console.error)
