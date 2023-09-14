<script setup lang="ts">
import { Plugin } from '../types'
import Entrypoint from './Entrypoint.vue'

defineProps<{plugin: Plugin}>()
</script>

<template>
  <div class="card">
    <div class="card-header">
      <div class="card-title">Plugin {{ plugin.name }}</div>
    </div>
    <div class="card-body">
      <ul class="list-group">
        <Entrypoint v-for="(q, i) in plugin.queries" :entry="q" :link="plugin.link" :key="'query-'+i" kind="query"/>
      </ul>
    </div>
  </div>
</template>
