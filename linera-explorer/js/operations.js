import template from '../html/operations.html'

export default function(r) {
    return {
        template,
        props: ["route", "operations"],
        methods: {
            operation_id(key) {
                return (r.short_crypto_hash(key.chain_id) + '-' + key.height + '-' + key.index)
            }
        }
    }
}
