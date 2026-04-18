<script setup lang="ts">
import { computed } from 'vue'
import { ConfirmedBlock } from '../../gql/service'
import { getOperations, getIncomingBundles } from './utils'
import Json from './Json.vue'
// Op is now imported by Transaction.vue
import Transaction from './Transaction.vue'
import OutgoingMessages from './OutgoingMessages.vue'

const props = defineProps<{block: ConfirmedBlock, title: string}>()

const operations = computed(() => getOperations(props.block.block.body.transactionMetadata))
const incomingBundles = computed(() => getIncomingBundles(props.block.block.body.transactionMetadata))
const transactions = computed(() => props.block.block.body.transactionMetadata || [])
</script>

<template>
  <div class="card">
    <div class="card-body">
      <h5 class="card-title">
        <span>{{ title }}</span>
        <button class="btn btn-link btn-sm" data-bs-toggle="modal" :data-bs-target="'#'+block.hash+'-modal'" @click="json_load(block.hash+'-json', block)">
          <i class="bi bi-braces"></i>
        </button>
        <div :id="block.hash+'-modal'" class="modal fade">
          <div class="modal-dialog modal-xl">
            <div class="modal-content">
              <div class="modal-header">
                <h5 class="modal-title">{{ title }}</h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
              </div>
              <div class="modal-body">
                <div :id="block.hash+'-json'" style="overflow-x: auto"></div>
              </div>
            </div>
          </div>
        </div>
      </h5>
      <ul class="list-group">
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Hash</strong></span>
          {{ block.hash }}
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Epoch</strong></span>
          <span>{{ block.block.header.epoch }}</span>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Height</strong></span>
          <span>{{ block.block.header.height }}</span>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Timestamp</strong></span>
          <span>{{ (new Date(Number(block.block.header.timestamp)/1000)).toLocaleString() }}</span>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Signer</strong></span>
          <span>{{ block.block.header.authenticatedOwner }}</span>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Previous Block</strong></span>
          <a v-if="block.block.header.previousBlockHash!==undefined" @click="$root.route('block', [['block', block.block.header.previousBlockHash]])" class="btn btn-link">{{ block.block.header.previousBlockHash }}</a>
          <span v-else>--</span>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>State Hash</strong></span>
          <span>{{ block.block.header.stateHash }}</span>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Status</strong></span>
          <span>{{ block.status }}</span>
        </li>
        <li v-if="transactions.length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#transactions-collapse-'+block.hash">
          <span><strong>Transactions</strong> ({{ transactions.length }})</span>
          <span class="text-muted ms-2">
            <small>
              {{ incomingBundles.length }} message{{ incomingBundles.length !== 1 ? 's' : '' }},
              {{ operations.length }} operation{{ operations.length !== 1 ? 's' : '' }}
            </small>
          </span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Transactions</strong> (0)</span>
          <span></span>
        </li>
        <div v-if="transactions.length!==0" class="collapse show" :id="'transactions-collapse-'+block.hash">
          <div class="p-3">
            <Transaction
              v-for="(tx, i) in transactions"
              :key="block.hash+'-tx-'+i"
              :transaction="tx"
              :index="i"
              :block-hash="block.hash"
            />
          </div>
        </div>
        <li v-if="block.block.body.messages.length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#out-messages-collapse-'+block.hash">
          <span><strong>Outgoing Messages</strong> ({{ block.block.body.messages.length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Outgoing Messages</strong> (0)</span>
          <span></span>
        </li>
        <div v-if="block.block.body.messages.length!==0" class="collapse" :id="'out-messages-collapse-'+block.hash">
          <div class="p-3">
            <OutgoingMessages
              v-for="(messages, i) in block.block.body.messages"
              :key="block.hash+'-outmessages-'+i"
              :messages="messages"
              :transaction-index="i"
              :block-hash="block.hash"
            />
          </div>
        </div>
        <li v-if="Object.keys(block.block.body.previousMessageBlocks).length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#previous-message-blocks-collapse-'+block.hash">
          <span><strong>Previous Message Blocks</strong> ({{ Object.keys(block.block.body.previousMessageBlocks).length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Previous Message Blocks</strong> (0)</span>
          <span></span>
        </li>
        <div v-if=" Object.keys(block.block.body.previousMessageBlocks).length!==0" class="collapse" :id="'previous-message-blocks-collapse-'+block.hash">
          <ul class="list-group">
            <li v-for="(hash, id) in block.block.body.previousMessageBlocks" class="list-group-item p-0" key="block.hash+'-previousmessageblock-'+id">
              <div class="card">
                <div class="card-header">Previous message from chain {{ id }} was sent at block</div>
                <div class="card-body">
                  <a @click="$root.route('block', [['block', hash]])" class="btn btn-link">{{ hash }}</a>
                </div>
              </div>
            </li>
          </ul>
        </div>
        <li v-if="block.block.body.oracleResponses.flat().length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#oracle-responses-collapse-'+block.hash">
          <span><strong>Oracle Responses</strong> ({{ block.block.body.oracleResponses.flat().length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Oracle Responses</strong> (0)</span>
          <span></span>
        </li>
        <div v-if="block.block.body.oracleResponses.flat().length!==0" class="collapse" :id="'oracle-responses-collapse-'+block.hash">
          <ul class="list-group">
            <template v-for="(txOracles, ti) in block.block.body.oracleResponses" :key="block.hash+'-oracles-tx-'+ti">
              <li v-for="(oracle, oi) in txOracles" class="list-group-item p-0" :key="block.hash+'-oracle-'+ti+'-'+oi">
                <div class="card border-0">
                  <div class="card-header">
                    <strong>Oracle Response</strong>
                    <span class="ms-2 small text-muted">Tx {{ ti+1 }}</span>
                  </div>
                  <div class="card-body small">
                    <Json :data="oracle"/>
                  </div>
                </div>
              </li>
            </template>
          </ul>
        </div>
        <li v-if="block.block.body.events.flat().length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#events-collapse-'+block.hash">
          <span><strong>Events</strong> ({{ block.block.body.events.flat().length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Events</strong> (0)</span>
          <span></span>
        </li>
        <div v-if="block.block.body.events.flat().length!==0" class="collapse" :id="'events-collapse-'+block.hash">
          <ul class="list-group">
            <template v-for="(txEvents, ti) in block.block.body.events" :key="block.hash+'-events-tx-'+ti">
              <li v-for="(evt, ei) in txEvents" class="list-group-item p-0" :key="block.hash+'-event-'+ti+'-'+ei">
                <div class="card border-0">
                  <div class="card-header">
                    <strong>Event</strong>
                    <span class="ms-2 small text-muted">Tx {{ ti+1 }}</span>
                    <span v-if="evt.streamId" class="ms-2 small">
                      Stream: <span class="font-monospace">{{ evt.streamId.applicationId ? short_app_id(evt.streamId.applicationId) : '' }}{{ evt.streamId.streamName ? '/' + evt.streamId.streamName : '' }}</span>
                    </span>
                    <span v-if="evt.index != null" class="ms-2 badge bg-secondary">index: {{ evt.index }}</span>
                  </div>
                  <div v-if="evt.value && evt.value.length > 0" class="card-body small">
                    <strong>Value:</strong> <span class="font-monospace">{{ evt.value }}</span>
                  </div>
                </div>
              </li>
            </template>
          </ul>
        </div>
        <li v-if="block.block.body.blobs.flat().length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#blobs-collapse-'+block.hash">
          <span><strong>Blobs</strong> ({{ block.block.body.blobs.flat().length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Blobs</strong> (0)</span>
          <span></span>
        </li>
        <div v-if="block.block.body.blobs.flat().length!==0" class="collapse" :id="'blobs-collapse-'+block.hash">
          <ul class="list-group">
            <template v-for="(txBlobs, ti) in block.block.body.blobs" :key="block.hash+'-blobs-tx-'+ti">
              <li v-for="(blob, bi) in txBlobs" class="list-group-item" :key="block.hash+'-blob-'+ti+'-'+bi">
                <div class="d-flex justify-content-between align-items-center" data-bs-toggle="collapse" :data-bs-target="'#blob-'+block.hash+'-'+ti+'-'+bi" role="button">
                  <span>
                    <strong>Blob {{ bi+1 }}</strong>
                    <span v-if="typeof blob === 'string'" class="ms-2 font-monospace small">{{ blob.length > 16 ? blob.substring(0, 8) + '..' + blob.substring(blob.length - 8) : blob }}</span>
                    <span v-else-if="Array.isArray(blob)" class="ms-2 font-monospace small">{{ blob.map((b: number) => b.toString(16).padStart(2, '0')).join('').substring(0, 8) + '..' }}</span>
                    <span class="ms-2 text-muted small">({{ typeof blob === 'string' ? blob.length / 2 : Array.isArray(blob) ? blob.length : '?' }} bytes)</span>
                  </span>
                  <i class="bi bi-caret-down-fill"></i>
                </div>
                <div class="collapse" :id="'blob-'+block.hash+'-'+ti+'-'+bi">
                  <div class="p-2 small font-monospace" style="word-break:break-all">
                    <span v-if="typeof blob === 'string'">{{ blob }}</span>
                    <span v-else-if="Array.isArray(blob)">{{ blob.map((b: number) => b.toString(16).padStart(2, '0')).join('') }}</span>
                  </div>
                </div>
              </li>
            </template>
          </ul>
        </div>
        <li v-if="block.block.body.operationResults.length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#operation-results-collapse-'+block.hash">
          <span><strong>Operation Results</strong> ({{ block.block.body.operationResults.length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Operation Results</strong> (0)</span>
          <span></span>
        </li>
        <div v-if="block.block.body.operationResults.length!==0" class="collapse" :id="'operation-results-collapse-'+block.hash">
          <ul class="list-group">
            <li v-for="(m, i) in block.block.body.operationResults" class="list-group-item" :key="block.hash+'-operationresult-'+i">
              <div class="d-flex justify-content-between align-items-center" data-bs-toggle="collapse" :data-bs-target="'#opresult-'+block.hash+'-'+i" role="button">
                <span>
                  <strong>Operation Result {{ i+1 }}</strong>
                  <span v-if="Array.isArray(m)" class="ms-2 font-monospace small">{{ m.map((b: number) => b.toString(16).padStart(2, '0')).join('').substring(0, 16) + (m.length > 8 ? '..' : '') }}</span>
                  <span v-else-if="typeof m === 'string'" class="ms-2 font-monospace small">{{ m.length > 16 ? m.substring(0, 8) + '..' + m.substring(m.length - 8) : m }}</span>
                  <span class="ms-2 text-muted small">({{ Array.isArray(m) ? m.length : typeof m === 'string' ? m.length / 2 : '?' }} bytes)</span>
                </span>
                <i class="bi bi-caret-down-fill"></i>
              </div>
              <div class="collapse" :id="'opresult-'+block.hash+'-'+i">
                <div class="p-2 small font-monospace" style="word-break:break-all">
                  <span v-if="Array.isArray(m)">{{ m.map((b: number) => b.toString(16).padStart(2, '0')).join('') }}</span>
                  <span v-else-if="typeof m === 'string'">{{ m }}</span>
                  <Json v-else :data="m"/>
                </div>
              </div>
            </li>
          </ul>
        </div>
      </ul>
    </div>
  </div>
</template>
