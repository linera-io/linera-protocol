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
          <span v-if="op.systemOperation?.systemOperationType" class="badge bg-primary ms-2">{{ op.systemOperation.systemOperationType }}</span>
        </div>
      </div>
      <div class="card-body">
        <!-- Transfer Operation -->
        <div v-if="op.systemOperation?.transfer" class="mb-3">
          <h6 class="text-primary">Transfer Details</h6>
          <div class="row">
            <div class="col-md-4">
              <strong>From:</strong>
              <div class="font-monospace text-break">{{ op.systemOperation.transfer.owner }}</div>
            </div>
            <div class="col-md-4">
              <strong>To:</strong>
              <div class="font-monospace text-break">{{ op.systemOperation.transfer.recipient }}</div>
            </div>
            <div class="col-md-4">
              <strong>Amount:</strong>
              <div class="font-monospace">{{ op.systemOperation.transfer.amount }}</div>
            </div>
          </div>
        </div>

        <!-- Claim Operation -->
        <div v-if="op.systemOperation?.claim" class="mb-3">
          <h6 class="text-primary">Claim Details</h6>
          <div class="row">
            <div class="col-md-3">
              <strong>Owner:</strong>
              <div class="font-monospace text-break">{{ op.systemOperation.claim.owner }}</div>
            </div>
            <div class="col-md-3">
              <strong>Target Chain:</strong>
              <div class="font-monospace text-break">{{ short_hash(op.systemOperation.claim.targetId) }}</div>
            </div>
            <div class="col-md-3">
              <strong>Recipient:</strong>
              <div class="font-monospace text-break">{{ op.systemOperation.claim.recipient }}</div>
            </div>
            <div class="col-md-3">
              <strong>Amount:</strong>
              <div class="font-monospace">{{ op.systemOperation.claim.amount }}</div>
            </div>
          </div>
        </div>

        <!-- Open Chain Operation -->
        <div v-if="op.systemOperation?.openChain" class="mb-3">
          <h6 class="text-primary">Open Chain Details</h6>
          <div class="row">
            <div class="col-md-4">
              <strong>Initial Balance:</strong>
              <div class="font-monospace">{{ op.systemOperation.openChain.balance }}</div>
            </div>
          </div>
        </div>

        <!-- Change Ownership Operation -->
        <div v-if="op.systemOperation?.changeOwnership" class="mb-3">
          <h6 class="text-primary">Change Ownership Details</h6>
          <div class="row">
            <div class="col-md-6">
              <strong>Super Owners ({{ op.systemOperation.changeOwnership.superOwners.length }}):</strong>
              <ul class="list-unstyled ms-2">
                <li v-for="owner in op.systemOperation.changeOwnership.superOwners" class="font-monospace small text-break">{{ owner }}</li>
              </ul>
            </div>
            <div class="col-md-6">
              <strong>Regular Owners ({{ op.systemOperation.changeOwnership.owners.length }}):</strong>
              <ul class="list-unstyled ms-2">
                <li v-for="ownerWeight in op.systemOperation.changeOwnership.owners" class="small">
                  <span class="font-monospace text-break">{{ ownerWeight.owner }}</span>
                  <span class="badge bg-secondary ms-1">weight: {{ ownerWeight.weight }}</span>
                </li>
              </ul>
            </div>
          </div>
          <div class="row mt-2">
            <div class="col-md-6">
              <strong>Multi-leader Rounds:</strong> {{ op.systemOperation.changeOwnership.multiLeaderRounds }}
            </div>
            <div class="col-md-6">
              <strong>Open Multi-leader:</strong> {{ op.systemOperation.changeOwnership.openMultiLeaderRounds ? 'Yes' : 'No' }}
            </div>
          </div>
        </div>

        <!-- Admin Operation -->
        <div v-if="op.systemOperation?.admin" class="mb-3">
          <h6 class="text-primary">Admin Operation Details</h6>
          <div class="row">
            <div class="col-md-4">
              <strong>Type:</strong> {{ op.systemOperation.admin.adminOperationType }}
            </div>
            <div class="col-md-4" v-if="op.systemOperation.admin.epoch !== null">
              <strong>Epoch:</strong> {{ op.systemOperation.admin.epoch }}
            </div>
            <div class="col-md-4" v-if="op.systemOperation.admin.blobHash">
              <strong>Blob Hash:</strong>
              <div class="font-monospace text-break small">{{ op.systemOperation.admin.blobHash }}</div>
            </div>
          </div>
        </div>

        <!-- Create Application Operation -->
        <div v-if="op.systemOperation?.createApplication" class="mb-3">
          <h6 class="text-primary">Create Application Details</h6>
          <div class="row">
            <div class="col-md-6">
              <strong>Module ID:</strong>
              <div class="font-monospace text-break small">{{ op.systemOperation.createApplication.moduleId }}</div>
            </div>
            <div class="col-md-6">
              <strong>Required Apps:</strong> {{ op.systemOperation.createApplication.requiredApplicationIds.length }}
            </div>
          </div>
          <div v-if="op.systemOperation.createApplication.parametersHex" class="mt-2">
            <details>
              <summary><strong>Parameters (hex)</strong></summary>
              <pre class="mt-2 p-2 bg-light small"><code>{{ op.systemOperation.createApplication.parametersHex }}</code></pre>
            </details>
          </div>
        </div>

        <!-- Publish Data Blob Operation -->
        <div v-if="op.systemOperation?.publishDataBlob" class="mb-3">
          <h6 class="text-primary">Publish Data Blob</h6>
          <div class="font-monospace text-break small">{{ op.systemOperation.publishDataBlob.blobHash }}</div>
        </div>

        <!-- Verify Blob Operation -->
        <div v-if="op.systemOperation?.verifyBlob" class="mb-3">
          <h6 class="text-primary">Verify Blob</h6>
          <div class="font-monospace text-break small">{{ op.systemOperation.verifyBlob.blobId }}</div>
        </div>

        <!-- Publish Module Operation -->
        <div v-if="op.systemOperation?.publishModule" class="mb-3">
          <h6 class="text-primary">Publish Module</h6>
          <div class="font-monospace text-break small">{{ op.systemOperation.publishModule.moduleId }}</div>
        </div>

        <!-- Epoch Operations -->
        <div v-if="op.systemOperation?.epoch !== null && op.systemOperation?.epoch !== undefined" class="mb-3">
          <h6 class="text-primary">{{ op.systemOperation.systemOperationType }} Details</h6>
          <div><strong>Epoch:</strong> {{ op.systemOperation.epoch }}</div>
        </div>

        <!-- Update Streams Operation -->
        <div v-if="op.systemOperation?.updateStreams" class="mb-3">
          <h6 class="text-primary">Update Streams Details</h6>
          <div class="table-responsive">
            <table class="table table-sm">
              <thead>
                <tr>
                  <th>Chain ID</th>
                  <th>Stream ID</th>
                  <th>Next Index</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="stream in op.systemOperation.updateStreams">
                  <td class="font-monospace text-break small">{{ short_hash(stream.chainId) }}</td>
                  <td class="font-monospace text-break small">{{ stream.streamId }}</td>
                  <td>{{ stream.nextIndex }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Change Application Permissions Operation -->
        <div v-if="op.systemOperation?.changeApplicationPermissions" class="mb-3">
          <h6 class="text-primary">Change Application Permissions</h6>
          <details>
            <summary><strong>Permissions</strong></summary>
            <pre class="mt-2 p-2 bg-light small"><code>{{ op.systemOperation.changeApplicationPermissions.permissionsJson }}</code></pre>
          </details>
        </div>

        <!-- Simple Operations -->
        <div v-if="op.systemOperation?.systemOperationType === 'CloseChain'" class="mb-3">
          <h6 class="text-primary">Close Chain</h6>
          <p class="text-muted">This operation closes the current chain.</p>
        </div>

        <!-- Fallback: show warning if structured data is not available -->
        <div v-if="!op.systemOperation" class="mb-3">
          <div class="alert alert-warning">
            <strong>Warning:</strong> No structured operation data available
          </div>
        </div>

        <!-- JSON view for debugging -->
        <details class="mt-3">
          <summary><strong>Raw JSON Data</strong></summary>
          <div class="mt-2">
            <Json :data="op"/>
          </div>
        </details>
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
