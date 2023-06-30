import template from '../html/output_type.html'

export default function(r) {
  return {
    name: 'v-output-type',
    template,
    props: ["t", "name", "depth"],
  }
}
