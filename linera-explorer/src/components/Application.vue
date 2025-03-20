<script setup lang="ts">
import { Application } from '../types'
import Entrypoint from './Entrypoint.vue'

defineProps<{app: Application}>()
</script>

<template>
  <div class="card">
    <div class="card-header">
      <div class="card-title">Application {{ short_app_id(app.app.address) }}</div>
    </div>
    <div class="card-body">
      <!-- QUERIES -->
      <div v-if="app.queries.length!=0">
        <h5 class="my-2">QUERIES</h5>
        <ul class="list-group">
          <Entrypoint v-for="(q, i) in app.queries" :entry="q" :link="app.app.link" :key="'query-'+i" kind="query"/>
        </ul>
      </div>
      <!-- MUTATIONS -->
      <div v-if="app.mutations.length!=0">
        <h5 class="my-2">MUTATIONS</h5>
        <ul class="list-group">
          <Entrypoint v-for="(m, i) in app.mutations" :entry="m" :link="app.app.link" :key="'mutation-'+i" kind="mutation"/>
        </ul>
      </div>
      <!-- SUBSCRIPTIONS -->
      <div v-if="app.subscriptions.length!=0">
        <h5 class="my-2">SUBSCRIPTIONS</h5>
        <ul class="list-group">
          <Entrypoint v-for="(s, i) in app.subscriptions" :entry="s" :link="app.app.link" :key="'subscription'+i" kind="subscription"/>
        </ul>
      </div>
    </div>
  </div>
</template>
