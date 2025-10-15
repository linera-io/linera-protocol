<script setup lang="ts">
import { TransactionMetadata } from '../../gql/service'
import Op from './Op.vue'
import Json from './Json.vue'

defineProps<{
  transaction: TransactionMetadata,
  index: number,
  blockHash: string
}>()
</script>

<template>
  <div class="card mb-2">
    <div class="card-header">
      <div class="d-flex justify-content-between align-items-center">
        <span>
          <strong>Transaction {{ index + 1 }}</strong>
          <span class="badge ms-2" :class="transaction.transactionType === 'ExecuteOperation' ? 'bg-primary' : 'bg-info'">
            {{ transaction.transactionType === 'ExecuteOperation' ? 'Execute Operation' : 'Receive Messages' }}
          </span>
        </span>
        <button class="btn btn-link btn-sm" data-bs-toggle="collapse" :data-bs-target="'#tx-details-' + blockHash + '-' + index">
          <i class="bi bi-chevron-down"></i>
        </button>
      </div>
    </div>

    <div class="collapse show" :id="'tx-details-' + blockHash + '-' + index">
      <div class="card-body">
        <!-- Execute Operation Transaction -->
        <div v-if="transaction.transactionType === 'ExecuteOperation' && transaction.operation">
          <Op :op="transaction.operation" :id="blockHash + '-tx-' + index" />
        </div>

        <!-- Receive Messages Transaction -->
        <div v-else-if="transaction.transactionType === 'ReceiveMessages' && transaction.incomingBundle">
          <div class="mb-3">
            <h6>Incoming Bundle from Chain</h6>
            <div class="row">
              <div class="col-md-6">
                <strong>Origin:</strong>
                <span class="font-monospace">{{ short_hash(transaction.incomingBundle.origin.sender || transaction.incomingBundle.origin) }}</span>
              </div>
              <div class="col-md-6">
                <strong>Action:</strong>
                <span class="badge bg-secondary">{{ transaction.incomingBundle.action }}</span>
              </div>
            </div>
          </div>

          <div v-if="transaction.incomingBundle.bundle" class="mt-3">
            <div class="row mb-2">
              <div class="col-md-4">
                <strong>Height:</strong> {{ transaction.incomingBundle.bundle.height }}
              </div>
              <div class="col-md-4">
                <strong>Transaction Index:</strong> {{ transaction.incomingBundle.bundle.transactionIndex }}
              </div>
              <div class="col-md-4">
                <strong>Certificate Hash:</strong>
                <span class="font-monospace small">{{ short_hash(transaction.incomingBundle.bundle.certificateHash) }}</span>
              </div>
            </div>

            <div v-if="transaction.incomingBundle.bundle.messages && transaction.incomingBundle.bundle.messages.length > 0">
              <h6 class="mt-3">Messages ({{ transaction.incomingBundle.bundle.messages.length }})</h6>
              <div v-for="(msg, msgIndex) in transaction.incomingBundle.bundle.messages" :key="msgIndex" class="border rounded p-2 mb-2">
                <div class="d-flex justify-content-between mb-2">
                  <span><strong>Message {{ msgIndex + 1 }}</strong></span>
                  <span class="badge bg-info">{{ msg.kind }}</span>
                </div>

                <!-- Display structured message metadata if available -->
                <div v-if="msg.messageMetadata">
                  <div v-if="msg.messageMetadata.messageType === 'System' && msg.messageMetadata.systemMessage">
                    <span class="badge bg-warning mb-2">System Message: {{ msg.messageMetadata.systemMessage.systemMessageType }}</span>

                    <!-- Credit message -->
                    <div v-if="msg.messageMetadata.systemMessage.credit" class="small">
                      <div><strong>Target:</strong> {{ msg.messageMetadata.systemMessage.credit.target }}</div>
                      <div><strong>Amount:</strong> {{ msg.messageMetadata.systemMessage.credit.amount }}</div>
                      <div><strong>Source:</strong> {{ msg.messageMetadata.systemMessage.credit.source }}</div>
                    </div>

                    <!-- Withdraw message -->
                    <div v-if="msg.messageMetadata.systemMessage.withdraw" class="small">
                      <div><strong>Owner:</strong> {{ msg.messageMetadata.systemMessage.withdraw.owner }}</div>
                      <div><strong>Amount:</strong> {{ msg.messageMetadata.systemMessage.withdraw.amount }}</div>
                      <div><strong>Recipient:</strong> {{ msg.messageMetadata.systemMessage.withdraw.recipient }}</div>
                    </div>
                  </div>

                  <div v-else-if="msg.messageMetadata.messageType === 'User'">
                    <span class="badge bg-success mb-2">User Message</span>
                    <div v-if="msg.messageMetadata.applicationId" class="small">
                      <strong>Application:</strong> {{ short_app_id(msg.messageMetadata.applicationId) }}
                    </div>
                  </div>
                </div>

                <!-- Fallback to raw message display -->
                <div v-else class="small">
                  <details>
                    <summary>Raw Message Data</summary>
                    <Json :data="msg" />
                  </details>
                </div>

                <div v-if="msg.grant && msg.grant !== '0'" class="mt-2 small">
                  <strong>Grant:</strong> {{ msg.grant }}
                </div>
              </div>
            </div>
          </div>

          <!-- Fallback for unknown structure -->
          <details v-else class="mt-3">
            <summary>Raw Bundle Data</summary>
            <Json :data="transaction.incomingBundle" />
          </details>
        </div>

        <!-- Unknown transaction type -->
        <div v-else>
          <div class="alert alert-warning">
            Unknown transaction type: {{ transaction.transactionType }}
          </div>
          <Json :data="transaction" />
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.font-monospace {
  font-family: 'Courier New', monospace;
}
.small {
  font-size: 0.875rem;
}
</style>