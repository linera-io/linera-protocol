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

        <li v-if="chain.inboxes.entries.length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#chain-'+chain.chainId+'-inboxes-collapse'">
          <span><strong>Inboxes</strong> ({{ chain.inboxes.entries.length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Inboxes</strong> (0)</span>
          <span></span>
        </li>
        <div v-if="chain.inboxes.entries.length!==0" class="collapse" :id="'chain-'+chain.chainId+'-inboxes-collapse'">
          <ul class="list-group">
            <li v-for="(inbox, i) in chain.inboxes.entries" class="list-group-item p-0" :key="'chain-'+chain.chainId+'-inbox-'+i">
              <div class="card border-0">
                <div class="card-header">
                  <strong>From</strong>
                  <a @click="$root.route(undefined, [['chain', inbox.key]])" class="btn btn-link btn-sm p-0 font-monospace">{{ short_hash(inbox.key) }}</a>
                </div>
                <div class="card-body">
                  <div class="row mb-2 small">
                    <div class="col-md-6">
                      <strong>Next cursor to add:</strong>
                      <span v-if="inbox.value.nextCursorToAdd" class="ms-1">h={{ inbox.value.nextCursorToAdd.height }}, i={{ inbox.value.nextCursorToAdd.index }}</span>
                    </div>
                    <div class="col-md-6">
                      <strong>Next cursor to remove:</strong>
                      <span v-if="inbox.value.nextCursorToRemove" class="ms-1">h={{ inbox.value.nextCursorToRemove.height }}, i={{ inbox.value.nextCursorToRemove.index }}</span>
                    </div>
                  </div>
                  <div v-if="inbox.value.addedBundles?.entries?.length" class="mb-2">
                    <details>
                      <summary class="small"><strong>Added Bundles</strong> ({{ inbox.value.addedBundles.entries.length }})</summary>
                      <div v-for="(bundle, bi) in inbox.value.addedBundles.entries" :key="bi" class="border rounded p-2 mb-1 mt-1 small">
                        <strong>Height:</strong> {{ bundle.height }}
                        <strong class="ms-2">Tx:</strong> {{ bundle.transactionIndex }}
                        <strong class="ms-2">Cert:</strong>
                        <a @click="$root.route('block', [['block', bundle.certificateHash]])" class="btn btn-link btn-sm p-0 font-monospace">{{ short_hash(bundle.certificateHash) }}</a>
                        <span v-if="bundle.messages?.length" class="text-muted ms-2">({{ bundle.messages.length }} msg{{ bundle.messages.length !== 1 ? 's' : '' }})</span>
                      </div>
                    </details>
                  </div>
                  <div v-if="inbox.value.removedBundles?.entries?.length">
                    <details>
                      <summary class="small"><strong>Removed Bundles</strong> ({{ inbox.value.removedBundles.entries.length }})</summary>
                      <div v-for="(bundle, bi) in inbox.value.removedBundles.entries" :key="bi" class="border rounded p-2 mb-1 mt-1 small">
                        <strong>Height:</strong> {{ bundle.height }}
                        <strong class="ms-2">Tx:</strong> {{ bundle.transactionIndex }}
                      </div>
                    </details>
                  </div>
                </div>
              </div>
            </li>
          </ul>
        </div>

        <li v-if="chain.outboxes.entries.length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#chain-'+chain.chainId+'-outboxes-collapse'">
          <span><strong>Outboxes</strong> ({{ chain.outboxes.entries.length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Outboxes</strong> (0)</span>
          <span></span>
        </li>
        <div v-if="chain.outboxes.entries.length!==0" class="collapse" :id="'chain-'+chain.chainId+'-outboxes-collapse'">
          <ul class="list-group">
            <li v-for="(outbox, i) in chain.outboxes.entries" class="list-group-item p-0" :key="'chain-'+chain.chainId+'-outbox-'+i">
              <div class="card border-0">
                <div class="card-header">
                  <strong>To</strong>
                  <a @click="$root.route(undefined, [['chain', outbox.key]])" class="btn btn-link btn-sm p-0 font-monospace">{{ short_hash(outbox.key) }}</a>
                </div>
                <div class="card-body small">
                  <div class="mb-2">
                    <strong>Next height to schedule:</strong>
                    <span class="ms-1">{{ outbox.value.nextHeightToSchedule }}</span>
                  </div>
                  <div v-if="outbox.value.queue?.entries?.length">
                    <details>
                      <summary><strong>Queue</strong> ({{ outbox.value.queue.entries.length }})</summary>
                      <Json :data="outbox.value.queue.entries"/>
                    </details>
                  </div>
                </div>
              </div>
            </li>
          </ul>
        </div>

      </ul>
    </div>
  </div>
</template>
