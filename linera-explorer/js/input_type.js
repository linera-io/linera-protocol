import template from '../html/input_type.html'

export default function(r) {
  return {
    name: 'v-input-type',
    template,
    props: ["t", "name"],
  }
}
