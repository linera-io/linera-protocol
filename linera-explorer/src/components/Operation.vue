<script setup lang="ts">
import { ChainOperation } from '../../gql/operations'
import Op from './Op.vue'

defineProps<{op: ChainOperation}>()
</script>

<template>
  <div class="card">
    <div class="card-body">
      <h5 class="card-title">
        <span>Operation</span>
        <button class="btn btn-link btn-sm" data-bs-toggle="modal" :data-bs-target="'#'+operation_id(op.key)+'-modal'" @click="json_load(operation_id(op.key)+'-json', op)">
          <i class="bi bi-braces"></i>
        </button>
        <div :id="operation_id(op.key)+'-modal'" class="modal fade">
          <div class="modal-dialog modal-xl">
            <div class="modal-content">
              <div class="modal-header">
                <h5 class="modal-title">Operation</h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
              </div>
              <div class="modal-body">
                <div :id="operation_id(op.key)+'-json'" style="overflow-x: auto"></div>
              </div>
            </div>
          </div>
        </div>
      </h5>
      <ul class="list-group">
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Index</strong></span>
          <span>{{ op.index }}</span>
        </li>

        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Block</strong></span>
          <a @click="$root.route('block', [['block', op.block]])" class="btn btn-link">{{ op.block }} ({{ op.key.index }})</a>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Height</strong></span>
          <span>{{ op.key.height }}</span>
        </li>
        <li class="list-group-item d-flex justify-content-between">
          <span><strong>Previous Operation</strong></span>
          <a v-if="op.previousOperation" @click="$root.route('operation', [['height', op.previousOperation.height.toString()], ['index', op.previousOperation.index.toString()]])" class="btn btn-link">{{ operation_id(op.previousOperation) }}</a>
          <span v-else>--</span>
        </li>
        <li class="list-group-item p-0">
          <Op :op="op.content" :id="operation_id(op.key)" :index="op.index"/>
        </li>
      </ul>
    </div>
  </div>
</template>
