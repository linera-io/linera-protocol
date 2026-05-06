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

// `serde_reflection` encodes a newtype struct like `Address32(pub [u8; 32])`
// or `CryptoHash([u8; 32])` as `{ "Address32": [b0, b1, ...] }`. Rendering 32
// numbered rows is unreadable; collapse such single-key wrappers around a
// fixed-size byte array into a `0x..` hex string.
const HEX_BYTE_LENGTHS = new Set([20, 32, 64])
// Bytes may arrive as numbers, BigInts, or numeric strings (the latter when
// the wasm serializer emitted BigInts that `safeStringify` then stringified).
function asByte(e: any): number | null {
  if (typeof e === 'number' && Number.isInteger(e) && e >= 0 && e <= 255) return e
  if (typeof e === 'bigint' && e >= 0n && e <= 255n) return Number(e)
  if (typeof e === 'string' && /^\d+$/.test(e)) {
    const n = Number(e)
    if (Number.isInteger(n) && n >= 0 && n <= 255) return n
  }
  return null
}
function asByteArray(v: any): number[] | null {
  if (!Array.isArray(v) || !HEX_BYTE_LENGTHS.has(v.length)) return null
  const bytes: number[] = []
  for (const e of v) {
    const b = asByte(e)
    if (b === null) return null
    bytes.push(b)
  }
  return bytes
}
function bytesToHex(bytes: number[]): string {
  return '0x' + bytes.map(b => b.toString(16).padStart(2, '0')).join('')
}
function prettify(value: any): any {
  if (Array.isArray(value)) {
    const bytes = asByteArray(value)
    if (bytes !== null) return bytesToHex(bytes)
    return value.map(prettify)
  }
  if (value && typeof value === 'object') {
    const keys = Object.keys(value)
    if (keys.length === 1) {
      const bytes = asByteArray(value[keys[0]])
      if (bytes !== null) return bytesToHex(bytes)
    }
    const out: Record<string, any> = {}
    for (const k of keys) out[k] = prettify(value[k])
    return out
  }
  return value
}

function render() {
  if (!container.value) return
  container.value.innerHTML = ''
  const raw = toRaw(props.data)
  if (raw === undefined || raw === null) return
  // Convert reactive proxy to plain object for JSONFormatter
  // Use BigInt-safe stringify since WASM serializer produces BigInt for u64/i64
  const plain = prettify(JSON.parse(safeStringify(raw)))
  let formatter = new JSONFormatter(plain, Infinity)
  container.value.appendChild(formatter.render())
}

onMounted(render)
watch(() => props.data, render, { deep: true })
</script>

<template>
  <div ref="container" style="overflow-x: auto"></div>
</template>
