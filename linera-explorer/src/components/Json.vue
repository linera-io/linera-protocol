<script setup lang="ts" >
import { ref, onMounted } from 'vue'
import JSONFormatter from 'json-formatter-js'

const props = defineProps<{data: any}>()
const container : any = ref(null)

onMounted(() => {
  let formatter = new JSONFormatter(props.data, Infinity)
  container.value!.appendChild(formatter.render())
})
</script>

<template>
  <div ref="container" style="overflow-x: auto"></div>
</template>
