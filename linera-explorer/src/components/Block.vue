<script setup lang="ts">
import { HashedConfirmedBlock } from '../../gql/service'
import { onMounted, ref } from 'vue'

defineProps<{blocks: HashedConfirmedBlock[]}>()

const currentLimit = ref('20')

onMounted(() => {
  const urlParams = new URLSearchParams(window.location.search)
  const limitParam = urlParams.get('limit')
  if (limitParam) {
    currentLimit.value = limitParam
  }
})

function changeLimit(event: Event) {
  const limit = (event.target as HTMLSelectElement).value;
  const chainId = new URLSearchParams(window.location.search).get('chain');
  if (chainId) {
    window.location.href = `/blocks?chain=${chainId}&limit=${limit}`;
  }
}
</script>

<template>
  <div v-if="blocks.length==0" class="text-center">
    No blocks for this chain
  </div>
  <div v-else>
    <div class="mb-3 d-flex justify-content-end">
      <div class="form-group">
        <label for="limitSelect" class="me-2">Blocks per page:</label>
        <select id="limitSelect" class="form-select form-select-sm d-inline-block w-auto" @change="changeLimit" :value="currentLimit">
          <option value="10">10</option>
          <option value="20">20</option>
          <option value="50">50</option>
          <option value="100">100</option>
        </select>
      </div>
    </div>
    <table class="table">
      <thead>
        <th>Height</th>
        <th>Hash</th>
        <th>Timestamp</th>
        <th>Signer</th>
        <th>Status</th>
        <th>#InMessages</th>
        <th>#OutMessages</th>
        <th>#Operations</th>
        <th>JSON</th>
      </thead>
      <tbody>
        <tr v-for="b in blocks" :key="'blocks-block-'+b.hash">
          <td>{{ b.value.block.header.height }}</td>
          <td :title="b.hash">
            <a @click="$root.route('block', [['block', b.hash]])" class="btn btn-link">{{ short_hash(b.hash) }}</a>
          </td>
          <td>{{ (new Date(b.value.block.header.timestamp/1000)).toLocaleString() }}</td>
          <td :title="b.value.block.header.authenticatedSigner">{{ short_hash(b.value.block.header.authenticatedSigner) }}</td>
          <td>{{ b.value.status }}</td>
          <td>{{ b.value.block.body.incomingBundles.length }}</td>
          <td>{{ b.value.block.body.messages.length }}</td>
          <td>{{ b.value.block.body.operations.length }}</td>
          <td>
            <button class="btn btn-link btn-sm" data-bs-toggle="modal" :data-bs-target="'#'+b.hash+'-modal'" @click="json_load(b.hash+'-json', b)">
              <i class="bi bi-braces"></i>
            </button>
            <div :id="b.hash+'-modal'" class="modal fade">
              <div class="modal-dialog modal-xl">
                <div class="modal-content">
                  <div class="modal-header">
                    <h5 class="modal-title">Block</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                  </div>
                  <div class="modal-body">
                    <div :id="b.hash+'-json'" style="overflow-x: auto">
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>
