import template from '../html/input_type.html'

export default function(r) {
  return {
    name: 'v-input-type',
    template,
    props: ["t", "offset"],
    methods: {
      append_input() { r.append_input(this) },
      remove_input(i) { r.remove_input(this, i) },
    }
  }
}
