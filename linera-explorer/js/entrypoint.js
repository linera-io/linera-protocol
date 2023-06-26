import template from '../html/entrypoint.html'

export default function(r) {
  return {
    template,
    data() {
      return { result: undefined, errors: undefined }
    },
    props: ["entry", "link", "kind"],
    methods: {
      query(args, kind) {
        r.query(this, args, kind)
      },
      empty_output(t) {
        return r.empty_output(t)
      },
    }
  }
}
