import JSONFormatter from 'json-formatter-js'

function load(id, data) {
  let formatter = new JSONFormatter(data, Infinity)
  let elt = document.getElementById(id)
  elt.appendChild(formatter.render())
}

var component = {
  template: '<div :id="inner_id" style="overflow-x: auto"></div>',
  props: [ "id", "data" ],
  data() { return { inner_id: this.id + '-inner' } },
  mounted() {
    load(this.inner_id, this.data)
  }
}

export default { load, component }
