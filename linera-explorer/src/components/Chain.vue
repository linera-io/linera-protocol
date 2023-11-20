<script setup lang="ts">
import { ChainStateExtendedView } from '../../gql/service'
import Json from './Json.vue'

defineProps<{title: string, chain: ChainStateExtendedView}>()
</script>

<template>
  <div class="card">
    <div class="card-body">
      <h5 class="card-title">
        <span>{{ title }}</span>
      </h5>
      <ul class="list-group">
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>ID</strong></span>
          {{ chain.chainId }}
        </li>

        <li v-if="chain.inboxes.values.length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#chain-'+chain.chainId+'-inboxes-collapse'">
          <span><strong>Inboxes</strong> ({{ chain.inboxes.values.length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Inboxes</strong> (0)</span>
          <span></span>
        </li>
        <div v-if="chain.inboxes.values.length!==0" class="collapse" :id="'chain-'+chain.chainId+'-inboxes-collapse'">
          <ul class="list-group">
            <li v-for="(inbox, i) in chain.inboxes.values" class="list-group-item p-0" key="'chain-'+chain.chainId+'-inbox-'+i">
              <div class="card">
                <div class="card-header">Inbox {{ i+1 }}</div>
                <div class="card-body">
                  <Json :data="inbox"/>
                </div>
              </div>
            </li>
          </ul>
        </div>

        <li v-if="chain.outboxes.values.length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#chain-'+chain.chainId+'-outboxes-collapse'">
          <span><strong>Outboxes</strong> ({{ chain.outboxes.values.length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Outboxes</strong> (0)</span>
          <span></span>
        </li>
        <div v-if="chain.outboxes.values.length!==0" class="collapse" :id="'chain-'+chain.chainId+'-outboxes-collapse'">
          <ul class="list-group">
            <li v-for="(outbox, i) in chain.outboxes.values" class="list-group-item p-0" key="'chain-'+chain.chainId+'-outbox-'+i">
              <div class="card">
                <div class="card-header">Outbox {{ i+1 }}</div>
                <div class="card-body">
                  <Json :data="outbox"/>
                </div>
              </div>
            </li>
          </ul>
        </div>

      </ul>
    </div>
  </div>
</template>
