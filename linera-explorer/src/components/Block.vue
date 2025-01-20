<script setup lang="ts">
import { HashedConfirmedBlock } from '../../gql/service'
import Json from './Json.vue'
import Op from './Op.vue'

defineProps<{block: HashedConfirmedBlock, title: string}>()
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
          <span>{{ block.value.block.header.epoch }}</span>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Height</strong></span>
          <span>{{ block.value.block.header.height }}</span>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Timestamp</strong></span>
          <span>{{ (new Date(block.value.block.header.timestamp/1000)).toLocaleString() }}</span>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Signer</strong></span>
          <span>{{ block.value.block.header.authenticatedSigner }}</span>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Previous Block</strong></span>
          <a v-if="block.value.block.header.previousBlockHash!==undefined" @click="$root.route('block', [['block', block.value.block.header.previousBlockHash]])" class="btn btn-link">{{ block.value.block.header.previousBlockHash }}</a>
          <span v-else>--</span>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>State Hash</strong></span>
          <span>{{ block.value.block.header.stateHash }}</span>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Status</strong></span>
          <span>{{ block.value.status }}</span>
        </li>
        <li v-if="block.value.block.body.incomingBundles.length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#in-messages-collapse-'+block.hash">
          <span><strong>Incoming Messages</strong> ({{ block.value.block.body.incomingBundles.length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Incoming Messages</strong> (0)</span>
          <span></span>
        </li>
        <div v-if="block.value.block.body.incomingBundles.length!==0" class="collapse" :id="'in-messages-collapse-'+block.hash">
          <ul class="list-group">
            <li v-for="(m, i) in block.value.block.body.incomingBundles" class="list-group-item p-0" key="block.hash+'-inmessage-'+i">
              <div class="card">
                <div class="card-header">Message {{ i+1 }}</div>
                <div class="card-body">
                  <Json :data="m"/>
                </div>
              </div>
            </li>
          </ul>
        </div>
        <li v-if="block.value.block.body.messages.length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#out-messages-collapse-'+block.hash">
          <span><strong>Outgoing Messages</strong> ({{ block.value.block.body.messages.length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Outgoing Messages</strong> (0)</span>
          <span></span>
        </li>
        <div v-if="block.value.block.body.messages.length!==0" class="collapse" :id="'out-messages-collapse-'+block.hash">
          <ul class="list-group">
            <li v-for="(m, i) in block.value.block.body.messages" class="list-group-item p-0" key="block.hash+'-outmessage-'+i">
              <div class="card">
                <div class="card-header">Messages for transaction {{ i+1 }}</div>
                <div class="card-body">
                  <Json :data="m"/>
                </div>
              </div>
            </li>
          </ul>
        </div>
        <li v-if="block.value.block.body.operations.length!==0" class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#operations-collapse-'+block.hash">
          <span><strong>Operations</strong> ({{ block.value.block.body.operations.length }})</span>
          <i class="bi bi-caret-down-fill"></i>
        </li>
        <li v-else class="list-group-item d-flex justify-content-between">
          <span><strong>Operations</strong> (0)</span>
          <span></span>
        </li>
        <div v-if="block.value.block.body.operations.length!==0" class="collapse" :id="'operations-collapse-'+block.hash">
          <ul class="list-group">
            <li v-for="(o, i) in block.value.block.body.operations" class="list-group-item p-0" key="block.hash+'-operation-'+i">
              <div class="card card-body p-0">
                <Op :op="o" :id="block.hash+'-operation-'+i" :index="i"/>
              </div>
            </li>
          </ul>
        </div>
      </ul>
    </div>
  </div>
</template>
