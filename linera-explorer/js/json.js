import JSONFormatter from 'json-formatter-js'

export default {
  template: '<div :id="inner_id"></div>',
  props: ["id", "data"],
  data() { return { inner_id: this.id + '-json' } },
  mounted() {
    let formatter = new JSONFormatter(this.data, Infinity)
    let elt = document.getElementById(this.inner_id)
    elt.appendChild(formatter.render())
  }
}
