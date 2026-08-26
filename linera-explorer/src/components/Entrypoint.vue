<script lang="ts">
import { query as rust_query, empty_response as rust_empty } from '../../pkg/linera_explorer'
import { defineComponent, PropType } from "vue"
import { IntrospectionField } from '../types'
import Json from './Json.vue'
import InputType from './InputType.vue'
import OutputType from './OutputType.vue'

const HASH_REGEX = /^[0-9a-f]{64}$/i

function parseLink(link: string): { base: string; chainId: string; appId: string } | null {
  const m = link.match(/^(.*?)\/chains\/([0-9a-fA-F]+)\/applications\/([0-9a-fA-F]+)$/)
  return m ? { base: m[1], chainId: m[2], appId: m[3] } : null
}

function bytesToHex(value: any): string | null {
  if (Array.isArray(value)) return value.map((b: number) => b.toString(16).padStart(2, '0')).join('')
  if (typeof value === 'string') return value
  return null
}

export default defineComponent({
  components: { Json, InputType, OutputType },
  props: {
    entry: {type: Object as PropType<IntrospectionField>, required: true},
    link: {type: String, required: true},
    kind: {type: String, required: true},
  },
  data() {
    return {
      result: undefined as any,
      errors: undefined as any,
      blockHash: null as string | null,
      decodedResponses: [] as { hex: string; decoded: any }[],
    }
  },
  watch: {
    result(newVal: any) {
      this.blockHash = null
      this.decodedResponses = []
      if (this.kind !== 'mutation') return
      if (typeof newVal !== 'string' || !HASH_REGEX.test(newVal)) return
      this.blockHash = newVal
      this.fetchAndDecode(newVal)
    }
  },
  methods: {
    query(args: any, kind: string) {
      rust_query(this, args, kind)
    },
    empty_response(t: any) : any {
      return rust_empty(t)
    },
    routeToBlock() {
      if (!this.blockHash) return
      const parsed = parseLink(this.link)
      const args: [string, string][] = [['block', this.blockHash]]
      if (parsed) args.push(['chain', parsed.chainId])
      ;(this.$root as any).route('block', args)
    },
    async fetchAndDecode(hash: string) {
      const parsed = parseLink(this.link)
      if (!parsed) return
      const body = JSON.stringify({
        query: `query Block($hash: CryptoHash, $chainId: ChainId!) {
          block(hash: $hash, chainId: $chainId) {
            block {
              body {
                operationResults
                transactionMetadata {
                  transactionType
                  operation { operationType applicationId }
                }
              }
            }
          }
        }`,
        variables: { hash, chainId: parsed.chainId }
      })
      let json: any
      try {
        const r = await fetch(parsed.base + '/', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body,
        })
        json = await r.json()
      } catch (_) { return }
      const ops = (json?.data?.block?.block?.body?.transactionMetadata ?? [])
        .filter((tx: any) => tx.transactionType === 'ExecuteOperation' && tx.operation)
        .map((tx: any) => tx.operation)
      const results: any[] = json?.data?.block?.block?.body?.operationResults ?? []
      const decoded: { hex: string; decoded: any }[] = []
      for (let i = 0; i < ops.length; i++) {
        const op = ops[i]
        if (op.operationType !== 'User' || op.applicationId !== parsed.appId) continue
        const hex = bytesToHex(results[i])
        if (hex === null) continue
        let dec: any = null
        try { dec = await (this.$root as any).decode_user_response(parsed.appId, hex) }
        catch (_) { /* leave dec as null */ }
        decoded.push({ hex, decoded: dec })
      }
      // Re-check the watched hash is still current — guards against an older
      // fetch landing after the user fired a newer mutation.
      if (this.blockHash === hash) this.decodedResponses = decoded
    },
  }
})
</script>

<template>
  <div>
    <li class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#query-collapse-'+entry.name">
      <span>{{ entry.name }}</span>
      <i class="bi bi-caret-down-fill"></i>
    </li>
    <div class="collapse" :id="'query-collapse-'+entry.name">
      <!-- INPUT -->
      <div class="card card-body" v-if="entry.args.length!=0">
        <div class="card-title">INPUT</div>
        <div v-for="a in entry.args" :key="'entrypoint-'+a.name">
          <div class="form-label" :title="a.description">{{ a.name }}{{ a.type.kind=='NON_NULL' ? ' (*)' : '' }}</div>
          <InputType :elt="a.type"/>
        </div>
      </div>
      <!-- OUTPUT -->
      <div class="card card-body" v-if="!empty_response(entry.type)">
        <div class="card-title">OUTPUT</div>
        <OutputType :elt="entry.type" :depth="0"/>
      </div>
      <div class="text-center">
        <button class="btn btn-primary m-2" @click="query(entry, kind)">SEND</button>
      </div>
      <!-- RESULT -->
      <div class="card card-body" v-if="result">
        <div class="card-title">RESULT</div>
        <div v-if="blockHash" class="font-monospace text-break">
          <span>Block </span>
          <a role="button" class="btn-link" @click="routeToBlock()">{{ blockHash }}</a>
        </div>
        <Json v-else :data="result"/>
        <div v-if="decodedResponses.length !== 0" class="mt-3">
          <div class="card-title">Decoded Response<span v-if="decodedResponses.length > 1">s</span></div>
          <div v-for="(d, i) in decodedResponses" :key="'decoded-'+i" class="border rounded p-2 mb-2">
            <Json v-if="d.decoded !== null && d.decoded !== undefined" :data="d.decoded"/>
            <div v-else class="font-monospace small text-muted" style="word-break:break-all">{{ d.hex || '(empty)' }}</div>
          </div>
        </div>
      </div>
      <!-- ERRORS -->
      <div class="card card-body" v-if="errors">
        <div class="card-title">ERRORS</div>
        <Json :data="errors"/>
      </div>
    </div>
  </div>
</template>
