<script setup lang="ts">
import { computed } from 'vue'
import { ChainStateExtendedView } from '../../gql/service'
import { formatTimestamp, displayValue, copyToClipboard } from './utils'
import Json from './Json.vue'

const props = defineProps<{title: string, chain: ChainStateExtendedView}>()

const sys = computed(() => props.chain.executionState?.system)

const description = computed(() => {
  const raw = sys.value?.description
  if (!raw) return null
  if (typeof raw === 'string') {
    try { return JSON.parse(raw) } catch { return raw }
  }
  return raw
})

function pendingBundleCount(): number {
  let count = 0
  for (const entry of props.chain.inboxes?.entries || []) {
    count += entry.value?.addedBundles?.entries?.length || 0
  }
  return count
}
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
          <span class="font-monospace text-break">{{ chain.chainId }} <a role="button" class="ms-1" @click="copyToClipboard(chain.chainId, $event)" title="Copy chain ID"><i class="bi bi-clipboard"></i></a></span>
        </li>

        <!-- System state -->
        <li v-if="sys" class="list-group-item d-flex justify-content-between">
          <span><strong>Balance</strong></span>
          <span>{{ displayValue(sys.balance) }}</span>
        </li>
        <li v-if="sys" class="list-group-item d-flex justify-content-between">
          <span><strong>Epoch</strong></span>
          <span>{{ displayValue(sys.epoch) }}</span>
        </li>
        <li v-if="sys" class="list-group-item d-flex justify-content-between">
          <span><strong>Timestamp</strong></span>
          <span>{{ formatTimestamp(sys.timestamp) }}</span>
        </li>
        <li v-if="sys?.adminChainId" class="list-group-item d-flex justify-content-between">
          <span><strong>Admin Chain</strong></span>
          <a @click="$root.route(undefined, [['chain', sys.adminChainId]])" class="btn btn-link btn-sm p-0 font-monospace">{{ short_hash(sys.adminChainId) }}</a>
        </li>
        <li v-if="description" class="list-group-item" data-bs-toggle="collapse" :data-bs-target="'#chain-'+chain.chainId+'-description-collapse'" role="button">
          <div class="d-flex justify-content-between">
            <span>
              <strong>Description</strong>
              <span v-if="description.origin" class="ms-2 badge bg-secondary">
                {{ description.origin.Root != null ? 'Root #' + description.origin.Root : 'Child' }}
              </span>
              <span v-if="description.config?.balance" class="ms-2 small text-muted">
                initial balance: {{ description.config.balance }}
              </span>
            </span>
            <i class="bi bi-caret-down-fill"></i>
          </div>
        </li>
        <div v-if="description" class="collapse" :id="'chain-'+chain.chainId+'-description-collapse'">
          <div class="list-group">
            <li v-if="description.origin" class="list-group-item d-flex justify-content-between small">
              <strong>Origin</strong>
              <span>{{ description.origin.Root != null ? 'Root chain #' + description.origin.Root : displayValue(description.origin) }}</span>
            </li>
            <li v-if="description.timestamp" class="list-group-item d-flex justify-content-between small">
              <strong>Created</strong>
              <span>{{ formatTimestamp(description.timestamp) }}</span>
            </li>
            <li v-if="description.config?.epoch != null" class="list-group-item d-flex justify-content-between small">
              <strong>Initial Epoch</strong>
              <span>{{ description.config.epoch }}</span>
            </li>
            <li v-if="description.config?.balance" class="list-group-item d-flex justify-content-between small">
              <strong>Initial Balance</strong>
              <span>{{ description.config.balance }}</span>
            </li>
            <li v-if="description.config?.ownership" class="list-group-item small">
              <details>
                <summary><strong>Initial Ownership</strong></summary>
                <div class="mt-1">
                  <div v-if="description.config.ownership.owners && Object.keys(description.config.ownership.owners).length">
                    <strong class="text-muted">Owners:</strong>
                    <div v-for="(weight, owner) in description.config.ownership.owners" :key="owner" class="ps-3 font-monospace text-break small">
                      {{ owner }} <span class="badge bg-secondary">weight {{ weight }}</span>
                    </div>
                  </div>
                  <div v-if="description.config.ownership.super_owners?.length">
                    <strong class="text-muted">Super Owners:</strong>
                    <div v-for="so in description.config.ownership.super_owners" :key="so" class="ps-3 font-monospace text-break small">{{ so }}</div>
                  </div>
                  <div class="mt-1 text-muted">
                    <span v-if="description.config.ownership.multi_leader_rounds != null">Multi-leader rounds: {{ description.config.ownership.multi_leader_rounds }}</span>
                    <span v-if="description.config.ownership.timeout_config" class="ms-3">
                      Timeout: base {{ description.config.ownership.timeout_config.base_timeout }},
                      incr {{ description.config.ownership.timeout_config.timeout_increment }}
                    </span>
                  </div>
                </div>
              </details>
            </li>
            <li v-if="description.config?.application_permissions" class="list-group-item small">
              <details>
                <summary><strong>Application Permissions</strong></summary>
                <Json :data="description.config.application_permissions"/>
              </details>
            </li>
          </div>
        </div>

        <!-- Tip state -->
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Next Block Height</strong></span>
          <span>{{ displayValue(chain.tipState.nextBlockHeight) }}</span>
        </li>
        <li v-if="chain.tipState.blockHash" class="list-group-item d-flex justify-content-between">
          <span><strong>Tip Block Hash</strong></span>
          <a @click="$root.route('block', [['block', chain.tipState.blockHash]])" class="btn btn-link btn-sm p-0 font-monospace">{{ short_hash(chain.tipState.blockHash) }}</a>
        </li>

        <!-- Manager / Consensus (#10) -->
        <li class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#chain-'+chain.chainId+'-manager-collapse'">
          <span>
            <strong>Consensus</strong>
            <span class="ms-2 badge bg-secondary">round {{ displayValue(chain.manager.currentRound) }}</span>
            <span v-if="chain.manager.roundTimeout" class="ms-1 badge bg-warning text-dark">timeout {{ formatTimestamp(chain.manager.roundTimeout) }}</span>
          </span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <div class="collapse" :id="'chain-'+chain.chainId+'-manager-collapse'">
          <div class="list-group">
            <li class="list-group-item d-flex justify-content-between small">
              <strong>Current Round</strong>
              <span>{{ displayValue(chain.manager.currentRound) }}</span>
            </li>
            <li class="list-group-item d-flex justify-content-between small">
              <strong>Seed</strong>
              <span>{{ displayValue(chain.manager.seed) }}</span>
            </li>
            <li v-if="chain.manager.roundTimeout" class="list-group-item d-flex justify-content-between small">
              <strong>Round Timeout</strong>
              <span>{{ formatTimestamp(chain.manager.roundTimeout) }}</span>
            </li>
            <li class="list-group-item small">
              <details>
                <summary><strong>Ownership</strong></summary>
                <Json :data="chain.manager.ownership"/>
              </details>
            </li>
            <li class="list-group-item small">
              <details>
                <summary><strong>Fallback Owners</strong></summary>
                <Json :data="chain.manager.fallbackOwners"/>
              </details>
            </li>
            <li v-if="chain.manager.lockingBlobs?.keys?.length" class="list-group-item small">
              <details>
                <summary><strong>Locking Blobs</strong> ({{ chain.manager.lockingBlobs.keys.length }})</summary>
                <Json :data="chain.manager.lockingBlobs.keys"/>
              </details>
            </li>
          </div>
        </div>

        <!-- Inboxes (#2) -->
        <li v-if="chain.inboxes.entries.length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#chain-'+chain.chainId+'-inboxes-collapse'">
          <span>
            <strong>Inboxes</strong> ({{ chain.inboxes.entries.length }})
            <span v-if="pendingBundleCount() > 0" class="ms-1 badge bg-warning text-dark">{{ pendingBundleCount() }} pending</span>
          </span>
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
                <div class="card-header d-flex justify-content-between align-items-center">
                  <span>
                    <strong>From</strong>
                    <a @click="$root.route(undefined, [['chain', inbox.key]])" class="btn btn-link btn-sm p-0 font-monospace">{{ short_hash(inbox.key) }}</a>
                  </span>
                  <span v-if="inbox.value.addedBundles?.entries?.length" class="badge bg-warning text-dark">{{ inbox.value.addedBundles.entries.length }} pending</span>
                </div>
                <div class="card-body">
                  <div class="row mb-2 small">
                    <div class="col-md-6">
                      <strong>Next cursor to add:</strong>
                      <span v-if="inbox.value.nextCursorToAdd" class="ms-1">h={{ displayValue(inbox.value.nextCursorToAdd.height) }}, i={{ displayValue(inbox.value.nextCursorToAdd.index) }}</span>
                    </div>
                    <div class="col-md-6">
                      <strong>Next cursor to remove:</strong>
                      <span v-if="inbox.value.nextCursorToRemove" class="ms-1">h={{ displayValue(inbox.value.nextCursorToRemove.height) }}, i={{ displayValue(inbox.value.nextCursorToRemove.index) }}</span>
                    </div>
                  </div>
                  <div v-if="inbox.value.addedBundles?.entries?.length" class="mb-2">
                    <details>
                      <summary class="small"><strong>Added Bundles</strong> ({{ inbox.value.addedBundles.entries.length }})</summary>
                      <div v-for="(bundle, bi) in inbox.value.addedBundles.entries" :key="bi" class="border rounded p-2 mb-1 mt-1 small">
                        <div class="d-flex justify-content-between align-items-center">
                          <span>
                            <strong>Height:</strong> {{ displayValue(bundle.height) }}
                            <strong class="ms-2">Tx:</strong> {{ displayValue(bundle.transactionIndex) }}
                            <strong class="ms-2">Cert:</strong>
                            <a @click="$root.route('block', [['block', bundle.certificateHash]])" class="btn btn-link btn-sm p-0 font-monospace">{{ short_hash(bundle.certificateHash) }}</a>
                          </span>
                          <span v-if="bundle.messages?.length" class="badge bg-info">{{ bundle.messages.length }} msg{{ bundle.messages.length !== 1 ? 's' : '' }}</span>
                        </div>
                        <div v-if="bundle.messages?.length">
                          <details class="mt-1">
                            <summary class="text-muted">Messages</summary>
                            <table class="table table-sm table-bordered mt-1 mb-0">
                              <thead>
                                <tr>
                                  <th>#</th>
                                  <th>Kind</th>
                                  <th>Type</th>
                                  <th>Details</th>
                                </tr>
                              </thead>
                              <tbody>
                                <tr v-for="(msg, mi) in bundle.messages" :key="mi">
                                  <td>{{ displayValue(msg.index) }}</td>
                                  <td>{{ displayValue(msg.kind) }}</td>
                                  <td>
                                    <span v-if="msg.messageMetadata?.messageType">{{ msg.messageMetadata.messageType }}</span>
                                    <span v-if="msg.messageMetadata?.applicationId" class="ms-1 font-monospace small">{{ short_app_id(msg.messageMetadata.applicationId) }}</span>
                                  </td>
                                  <td>
                                    <span v-if="msg.messageMetadata?.systemMessage?.systemMessageType">
                                      {{ msg.messageMetadata.systemMessage.systemMessageType }}
                                      <span v-if="msg.messageMetadata.systemMessage.credit" class="ms-1">
                                        {{ displayValue(msg.messageMetadata.systemMessage.credit.amount) }} to {{ short_hash(msg.messageMetadata.systemMessage.credit.target) }}
                                      </span>
                                      <span v-if="msg.messageMetadata.systemMessage.withdraw" class="ms-1">
                                        {{ displayValue(msg.messageMetadata.systemMessage.withdraw.amount) }}
                                      </span>
                                    </span>
                                    <span v-else-if="msg.grant && msg.grant !== '0.'" class="text-muted">grant: {{ displayValue(msg.grant) }}</span>
                                  </td>
                                </tr>
                              </tbody>
                            </table>
                          </details>
                        </div>
                      </div>
                    </details>
                  </div>
                  <div v-if="inbox.value.removedBundles?.entries?.length">
                    <details>
                      <summary class="small"><strong>Removed Bundles</strong> ({{ inbox.value.removedBundles.entries.length }})</summary>
                      <div v-for="(bundle, bi) in inbox.value.removedBundles.entries" :key="bi" class="border rounded p-2 mb-1 mt-1 small">
                        <div class="d-flex justify-content-between align-items-center">
                          <span>
                            <strong>Height:</strong> {{ displayValue(bundle.height) }}
                            <strong class="ms-2">Tx:</strong> {{ displayValue(bundle.transactionIndex) }}
                            <strong v-if="bundle.certificateHash" class="ms-2">Cert:</strong>
                            <a v-if="bundle.certificateHash" @click="$root.route('block', [['block', bundle.certificateHash]])" class="btn btn-link btn-sm p-0 font-monospace">{{ short_hash(bundle.certificateHash) }}</a>
                          </span>
                          <span v-if="bundle.messages?.length" class="badge bg-secondary">{{ bundle.messages.length }} msg{{ bundle.messages.length !== 1 ? 's' : '' }}</span>
                        </div>
                        <div v-if="bundle.messages?.length">
                          <details class="mt-1">
                            <summary class="text-muted">Messages</summary>
                            <table class="table table-sm table-bordered mt-1 mb-0">
                              <thead>
                                <tr>
                                  <th>#</th>
                                  <th>Kind</th>
                                  <th>Type</th>
                                  <th>Details</th>
                                </tr>
                              </thead>
                              <tbody>
                                <tr v-for="(msg, mi) in bundle.messages" :key="mi">
                                  <td>{{ displayValue(msg.index) }}</td>
                                  <td>{{ displayValue(msg.kind) }}</td>
                                  <td>
                                    <span v-if="msg.messageMetadata?.messageType">{{ msg.messageMetadata.messageType }}</span>
                                    <span v-if="msg.messageMetadata?.applicationId" class="ms-1 font-monospace small">{{ short_app_id(msg.messageMetadata.applicationId) }}</span>
                                  </td>
                                  <td>
                                    <span v-if="msg.messageMetadata?.systemMessage?.systemMessageType">
                                      {{ msg.messageMetadata.systemMessage.systemMessageType }}
                                    </span>
                                  </td>
                                </tr>
                              </tbody>
                            </table>
                          </details>
                        </div>
                      </div>
                    </details>
                  </div>
                </div>
              </div>
            </li>
          </ul>
        </div>

        <!-- Outboxes (#2) -->
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
                <div class="card-header d-flex justify-content-between align-items-center">
                  <span>
                    <strong>To</strong>
                    <a @click="$root.route(undefined, [['chain', outbox.key]])" class="btn btn-link btn-sm p-0 font-monospace">{{ short_hash(outbox.key) }}</a>
                  </span>
                  <span v-if="outbox.value.queue?.entries?.length" class="badge bg-warning text-dark">{{ outbox.value.queue.entries.length }} queued</span>
                </div>
                <div class="card-body small">
                  <div class="mb-2">
                    <strong>Next height to schedule:</strong>
                    <span class="ms-1">{{ displayValue(outbox.value.nextHeightToSchedule) }}</span>
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

        <!-- Outbox Counters -->
        <li v-if="chain.outboxCounters && Object.keys(chain.outboxCounters).length" class="list-group-item small">
          <details>
            <summary><strong>Outbox Counters</strong></summary>
            <Json :data="chain.outboxCounters"/>
          </details>
        </li>

        <!-- Block Hashes -->
        <li v-if="chain.blockHashes?.entries?.length" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#chain-'+chain.chainId+'-blockhashes-collapse'">
          <span><strong>Block Hashes</strong> ({{ chain.blockHashes.entries.length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <div v-if="chain.blockHashes?.entries?.length" class="collapse" :id="'chain-'+chain.chainId+'-blockhashes-collapse'">
          <div class="list-group small">
            <a v-for="entry in chain.blockHashes.entries" :key="entry.key"
               @click="$root.route('block', [['block', entry.value]])"
               class="list-group-item list-group-item-action font-monospace p-1 ps-3">
              {{ entry.key }}: {{ short_hash(entry.value) }}
            </a>
          </div>
        </div>

        <!-- Received Log -->
        <li v-if="chain.receivedLog?.entries?.length" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#chain-'+chain.chainId+'-receivedlog-collapse'">
          <span><strong>Received Log</strong> ({{ chain.receivedLog.entries.length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <div v-if="chain.receivedLog?.entries?.length" class="collapse" :id="'chain-'+chain.chainId+'-receivedlog-collapse'">
          <div class="list-group small">
            <div v-for="(entry, i) in chain.receivedLog.entries" :key="i" class="list-group-item p-1 ps-3">
              <a @click="$root.route(undefined, [['chain', entry.chainId]])" class="btn btn-link btn-sm p-0 font-monospace">{{ short_hash(entry.chainId) }}</a>
              <span class="ms-1">height {{ displayValue(entry.height) }}</span>
            </div>
          </div>
        </div>

      </ul>
    </div>
  </div>
</template>
