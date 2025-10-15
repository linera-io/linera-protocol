<script setup lang="ts">
import Json from './Json.vue'

defineProps<{
  messages: any[] // Vec<OutgoingMessage>
  transactionIndex: number
  blockHash: string
}>()

// Helper function to extract message metadata from the message field
function getMessageMetadata(msg: any) {
  if (!msg || !msg.message) return null

  let message = msg.message

  // If message is a Map, convert it to a plain object
  if (message instanceof Map) {
    message = Object.fromEntries(message)
  }

  // If message is a string, try to parse it as JSON
  if (typeof message === 'string') {
    try {
      message = JSON.parse(message)
    } catch (e) {
      console.error('Failed to parse message as JSON:', e)
      return null
    }
  }

  // Check if it's a System message
  if (message.System) {
    let systemMsg = message.System

    // If systemMsg is a Map, convert it to a plain object
    if (systemMsg instanceof Map) {
      systemMsg = Object.fromEntries(systemMsg)
    }

    const systemMessageType = Object.keys(systemMsg)[0]
    const result: any = {
      messageType: 'System',
      systemMessage: {
        systemMessageType,
        data: systemMsg[systemMessageType]
      }
    }

    // Handle Credit message
    if (systemMsg.Credit) {
      let credit = systemMsg.Credit
      if (credit instanceof Map) {
        credit = Object.fromEntries(credit)
      }
      result.systemMessage.credit = credit
    }

    // Handle Withdraw message
    if (systemMsg.Withdraw) {
      let withdraw = systemMsg.Withdraw
      if (withdraw instanceof Map) {
        withdraw = Object.fromEntries(withdraw)
      }
      result.systemMessage.withdraw = withdraw
    }

    return result
  }

  // Check if it's a User message
  if (message.User) {
    return {
      messageType: 'User',
      applicationId: message.User.application_id,
      userBytesHex: message.User.bytes
    }
  }

  return null
}
</script>

<template>
  <div class="card mb-2">
    <div class="card-header">
      <div class="d-flex justify-content-between align-items-center">
        <span>
          <strong>Messages for Transaction {{ transactionIndex + 1 }}</strong>
          <span class="badge bg-primary ms-2">{{ messages.length }} message{{ messages.length !== 1 ? 's' : '' }}</span>
        </span>
        <button class="btn btn-link btn-sm" data-bs-toggle="collapse" :data-bs-target="'#out-msgs-' + blockHash + '-' + transactionIndex">
          <i class="bi bi-chevron-down"></i>
        </button>
      </div>
    </div>

    <div class="collapse show" :id="'out-msgs-' + blockHash + '-' + transactionIndex">
      <div class="card-body">
        <div v-for="(msg, msgIndex) in messages" :key="msgIndex" class="border rounded p-2 mb-2">
          <div class="d-flex justify-content-between mb-2">
            <span><strong>Message {{ msgIndex + 1 }}</strong></span>
            <span class="badge bg-info">{{ msg.kind }}</span>
          </div>

          <div class="mb-2 small">
            <strong>Destination:</strong>
            <span class="font-monospace" v-if="typeof msg.destination === 'string'">{{ short_hash(msg.destination) }}</span>
            <span v-else>{{ JSON.stringify(msg.destination) }}</span>
          </div>

          <!-- Display structured message metadata if available -->
          <div v-if="getMessageMetadata(msg)" class="mb-2">
            <div v-if="getMessageMetadata(msg).messageType === 'System' && getMessageMetadata(msg).systemMessage">
              <span class="badge bg-warning mb-2">System Message: {{ getMessageMetadata(msg).systemMessage.systemMessageType }}</span>

              <!-- Credit message -->
              <div v-if="getMessageMetadata(msg).systemMessage.credit" class="small">
                <div><strong>Target:</strong> {{ getMessageMetadata(msg).systemMessage.credit.target }}</div>
                <div><strong>Amount:</strong> {{ getMessageMetadata(msg).systemMessage.credit.amount }}</div>
                <div><strong>Source:</strong> {{ getMessageMetadata(msg).systemMessage.credit.source }}</div>
              </div>

              <!-- Withdraw message -->
              <div v-else-if="getMessageMetadata(msg).systemMessage.withdraw" class="small">
                <div><strong>Owner:</strong> {{ getMessageMetadata(msg).systemMessage.withdraw.owner }}</div>
                <div><strong>Amount:</strong> {{ getMessageMetadata(msg).systemMessage.withdraw.amount }}</div>
                <div><strong>Recipient:</strong> {{ getMessageMetadata(msg).systemMessage.withdraw.recipient }}</div>
              </div>

              <!-- Generic system message data for other types -->
              <div v-else class="small">
                <Json :data="getMessageMetadata(msg).systemMessage.data" />
              </div>
            </div>

            <div v-else-if="getMessageMetadata(msg).messageType === 'User'">
              <span class="badge bg-success mb-2">User Message</span>
              <div v-if="getMessageMetadata(msg).applicationId" class="small">
                <strong>Application:</strong> {{ short_app_id(getMessageMetadata(msg).applicationId) }}
              </div>
            </div>
          </div>

          <!-- Fallback to raw message display -->
          <div v-else class="small">
            <details>
              <summary>Raw Message Data (No Metadata)</summary>
              <Json :data="msg" />
            </details>
          </div>

          <div v-if="msg.grant && msg.grant !== '0'" class="mt-2 small">
            <strong>Grant:</strong> {{ msg.grant }}
          </div>

          <div v-if="msg.authenticatedOwner" class="mt-2 small">
            <strong>Authenticated Owner:</strong> {{ msg.authenticatedOwner }}
          </div>
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
