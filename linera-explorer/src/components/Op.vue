<script setup lang="ts">
import Json from './Json.vue'

defineProps<{op: any, id: string, index?: number}>()
</script>

<template>
  <div v-if="op?.operationType === 'System'">
    <div class="card">
      <div class="card-header">
        <div class="card-title">
          <span v-if="index!==undefined">{{ index+1 }}. </span>
          <span>System Operation</span>
        </div>
      </div>
      <div class="card-body">
        <div v-if="op.systemBytesHex" class="mb-3">
          <strong>Operation Data (hex):</strong>
          <pre class="mt-2 p-2 bg-light"><code>{{ op.systemBytesHex }}</code></pre>
        </div>
        <Json :data="op"/>
      </div>
    </div>
  </div>

  <div v-else-if="op?.operationType === 'User'">
    <div class="card">
      <div class="card-header">
        <span v-if="index!==undefined">{{ index+1 }}. </span>
        <span>User Operation</span>
        <span v-if="op.applicationId" class="badge bg-secondary ms-2">{{ short_app_id(op.applicationId) }}</span>
      </div>
      <div class="card-body">
        <div v-if="op.applicationId" class="mb-2">
          <strong>Application ID:</strong>
          <span class="font-monospace">{{ op.applicationId }}</span>
        </div>
        <div v-if="op.userBytesHex" class="mb-3">
          <strong>Operation Data (hex):</strong>
          <pre class="mt-2 p-2 bg-light"><code>{{ op.userBytesHex }}</code></pre>
        </div>
        <Json :data="op"/>
      </div>
    </div>
  </div>

  <div v-else>
    <div class="card">
      <div class="card-header">
        <span v-if="index!==undefined">{{ index+1 }}. </span>
        <span>Unknown Operation</span>
        <span v-if="op?.operationType" class="badge bg-warning ms-2">{{ op.operationType }}</span>
      </div>
      <div class="card-body">
        <div class="alert alert-info">
          <strong>Operation Type:</strong> {{ op?.operationType || 'Unknown' }}
        </div>
        <Json :data="op"/>
      </div>
    </div>
  </div>
</template>
