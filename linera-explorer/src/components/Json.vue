<script setup lang="ts" >
import { ref, onMounted, watch, toRaw } from 'vue'
import JSONFormatter from 'json-formatter-js'

const props = defineProps<{data: any}>()
const container : any = ref(null)

function safeStringify(obj: any): string {
  return JSON.stringify(obj, (_key, value) =>
    typeof value === 'bigint' ? value.toString() : value
  )
}

function render() {
  if (!container.value) return
  container.value.innerHTML = ''
  const raw = toRaw(props.data)
  if (raw === undefined || raw === null) return
  // Convert reactive proxy to plain object for JSONFormatter
  // Use BigInt-safe stringify since WASM serializer produces BigInt for u64/i64
  const plain = JSON.parse(safeStringify(raw))
  let formatter = new JSONFormatter(plain, Infinity)
  container.value.appendChild(formatter.render())
}

onMounted(render)
watch(() => props.data, render, { deep: true })
</script>

<template>
  <div ref="container" style="overflow-x: auto"></div>
</template>
