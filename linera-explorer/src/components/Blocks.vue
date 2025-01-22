<script setup lang="ts">
import { HashedConfirmedBlock } from '../../gql/service'

defineProps<{blocks: HashedConfirmedBlock[]}>()
</script>

<template>
  <div v-if="blocks.length==0" class="text-center">
    No blocks for this chain
  </div>
  <div v-else>
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
