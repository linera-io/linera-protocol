<script setup lang="ts">
defineProps<{transfer: {result?: string}}>()
</script>

<template>
  <div class="card">
    <div class="card-header">Transfer</div>
    <div class="card-body">
      <div v-if="transfer.result" class="alert alert-success">{{ transfer.result }}</div>
      <form @submit.prevent="$root.route('transfer', [
        ['owner', ($refs.owner as HTMLInputElement).value],
        ['recipient_chain', ($refs.recipientChain as HTMLInputElement).value],
        ['recipient_owner', ($refs.recipientOwner as HTMLInputElement).value || ($refs.owner as HTMLInputElement).value],
        ['amount', ($refs.amount as HTMLInputElement).value],
      ])">
        <div class="mb-3">
          <label class="form-label"><strong>Owner</strong> (sender account owner)</label>
          <input ref="owner" type="text" class="form-control font-monospace small" placeholder="0x..." required>
          <div class="form-text">The AccountOwner on the current chain</div>
        </div>
        <div class="mb-3">
          <label class="form-label"><strong>Recipient Chain</strong></label>
          <input ref="recipientChain" type="text" class="form-control font-monospace small" placeholder="chain id..." required>
        </div>
        <div class="mb-3">
          <label class="form-label"><strong>Recipient Owner</strong> (optional, defaults to sender)</label>
          <input ref="recipientOwner" type="text" class="form-control font-monospace small" placeholder="0x... (leave empty to use sender)">
        </div>
        <div class="mb-3">
          <label class="form-label"><strong>Amount</strong></label>
          <input ref="amount" type="text" class="form-control" placeholder="e.g. 100" required>
        </div>
        <button type="submit" class="btn btn-primary">Send Transfer</button>
      </form>
    </div>
  </div>
</template>
