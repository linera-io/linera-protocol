<script setup lang="ts">
import { ref } from 'vue'

const owner = ref('0x00')
const recipientChain = ref('')
const recipientOwner = ref('0x00')
const amount = ref('')
const status = ref('')
const sending = ref(false)

const emit = defineEmits<{
  (e: 'transfer', payload: { owner: string, recipientChain: string, recipientOwner: string, amount: string }): void
}>()

function submitTransfer() {
  if (!amount.value || !recipientChain.value) {
    status.value = 'Please fill in recipient chain and amount'
    return
  }
  sending.value = true
  status.value = ''
  emit('transfer', {
    owner: owner.value,
    recipientChain: recipientChain.value,
    recipientOwner: recipientOwner.value,
    amount: amount.value,
  })
}

defineExpose({ status, sending })
</script>

<template>
  <div class="card">
    <div class="card-header d-flex justify-content-between align-items-center" data-bs-toggle="collapse" data-bs-target="#transfer-form-collapse" role="button">
      <strong>Transfer Tokens</strong>
      <i class="bi bi-caret-down-fill"></i>
    </div>
    <div class="collapse" id="transfer-form-collapse">
      <div class="card-body">
        <div class="row g-3">
          <div class="col-md-6">
            <label class="form-label"><strong>From (Owner)</strong></label>
            <input v-model="owner" class="form-control font-monospace" placeholder="0x00 = chain account">
            <small class="text-muted">Use 0x00 for the chain account</small>
          </div>
          <div class="col-md-6">
            <label class="form-label"><strong>Amount</strong></label>
            <input v-model="amount" class="form-control" type="text" placeholder="e.g. 1.0">
          </div>
          <div class="col-md-8">
            <label class="form-label"><strong>Recipient Chain ID</strong></label>
            <input v-model="recipientChain" class="form-control font-monospace" placeholder="Destination chain ID">
          </div>
          <div class="col-md-4">
            <label class="form-label"><strong>Recipient Owner</strong></label>
            <input v-model="recipientOwner" class="form-control font-monospace" placeholder="0x00">
          </div>
        </div>
        <div class="mt-3 d-flex align-items-center gap-3">
          <button class="btn btn-primary" @click="submitTransfer" :disabled="sending">
            <span v-if="sending" class="spinner-border spinner-border-sm me-1"></span>
            {{ sending ? 'Sending...' : 'Send Transfer' }}
          </button>
          <span v-if="status" :class="status.startsWith('Error') ? 'text-danger' : 'text-success'">{{ status }}</span>
        </div>
      </div>
    </div>
  </div>
</template>
