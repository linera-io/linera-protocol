<script setup lang="ts">
import Json from './Json.vue'

defineProps<{op: any, id: string, index?: number}>()
</script>

<template>
  <div v-if="op?.System">
    <div v-if="op.System.Transfer">
      <div class="card">
        <div class="card-header">
          <div class="card-title">
            <span v-if="index!==undefined">{{ index+1 }}. </span>
            <span>Transfer</span>
          </div>
        </div>
        <div class="card-body d-flex justify-content-around">
          <span>{{ op.System.Transfer.amount }}</span>
          <i class="bi bi-arrow-right"></i>
          <a :title="op.System.Transfer.recipient.Account.chain_id" @click="$root.route('blocks', [['chain', op.System.Transfer.recipient.Account.chain_id]])" class="btn btn-link">
            {{ short_hash(op.System.Transfer.recipient.Account.chain_id) }}
          </a>
        </div>
      </div>
    </div>
    <div v-else>
      <div class="card">
        <div class="card-header">
          <span v-if="index!==undefined">{{ index+1 }}. </span>
          <span>Operation</span>
        </div>
        <div class="card-body">
          <Json :data="op"/>
        </div>
      </div>
    </div>
  </div>

  <div v-else-if="op?.User">
    <div class="card">
      <div class="card-header">
        <span v-if="index!==undefined">{{ index+1 }}. </span>
        <span>Application</span>
      </div>
      <div class="card-body">
        <pre><code>{{ op.User.bytes }}</code></pre>
      </div>
    </div>
  </div>

  <div v-else>
    <div class="card">
      <div class="card-header">
        <span v-if="index!==undefined">{{ index+1 }}. </span>
        <span>Undefined operation</span>
      </div>
    </div>
  </div>
</template>
