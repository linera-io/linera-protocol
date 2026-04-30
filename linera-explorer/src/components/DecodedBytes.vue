<script setup lang="ts">
import { ref, watch, getCurrentInstance } from 'vue'
import Json from './Json.vue'

const props = defineProps<{
  applicationId: string,
  bytesHex: string,
  // The kind of payload to decode. Maps 1:1 to the wasm-bindgen decode entry
  // points exposed by `App.vue` (`decode_user_<kind>`).
  kind: 'operation' | 'message' | 'response' | 'event_value',
  // Heading shown above the decoded JSON. Defaults to a sensible per-kind
  // value but callers can override it.
  label?: string
}>()

const decoded = ref<any>(null)
const decoding = ref(false)

function methodName() {
  switch (props.kind) {
    case 'operation': return 'decode_user_operation'
    case 'message': return 'decode_user_message'
    case 'response': return 'decode_user_response'
    case 'event_value': return 'decode_user_event_value'
  }
}

function defaultLabel() {
  switch (props.kind) {
    case 'operation': return 'Decoded Operation:'
    case 'message': return 'Decoded Message:'
    case 'response': return 'Decoded Response:'
    case 'event_value': return 'Decoded Event:'
  }
}

async function tryDecode() {
  decoded.value = null
  if (!props.applicationId || !props.bytesHex) return
  const root: any = getCurrentInstance()?.proxy?.$root
  if (!root?.config?.formats_registry) return
  const fn = root[methodName()]
  if (typeof fn !== 'function') return
  decoding.value = true
  try {
    const value = await fn.call(root, props.applicationId, props.bytesHex)
    decoded.value = value ?? null
  } finally {
    decoding.value = false
  }
}

watch(() => [props.applicationId, props.bytesHex, props.kind], tryDecode, { immediate: true })
</script>

<template>
  <div v-if="decoding" class="text-muted small">
    <span class="spinner-border spinner-border-sm me-2"></span>Decoding...
  </div>
  <div v-else-if="decoded !== null">
    <strong>{{ label ?? defaultLabel() }}</strong>
    <div class="mt-2"><Json :data="decoded"/></div>
  </div>
</template>
