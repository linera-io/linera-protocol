const rust = import("../pkg/index.js")

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
  }).component('v-block', block_component).mount("#app")
  r.init(app)
}

rust.then(r => main(r)).catch(console.error)
