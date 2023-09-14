import template from '../html/operation.html'

export default function(r) {
    return {
        template,
        props: ["op", "route"],
        methods: {
            operation_id(key) {
                return (r.short_crypto_hash(key.chain_id) + '-' + key.height + '-' + key.index)
            }
        }

    }
}
