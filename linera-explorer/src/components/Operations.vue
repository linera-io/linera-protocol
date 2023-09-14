<script setup lang="ts">
import { ChainOperation } from '../../gql/operations'

defineProps<{operations: ChainOperation[]}>()
</script>

<template>
  <div v-if="operations.length==0" class="text-center">
    No operations for this chain
  </div>
  <div v-else>
    <table class="table">
      <thead>
        <th>Index</th>
        <th>Block</th>
        <th>Height</th>
        <th>Previous</th>
        <th>JSON</th>
      </thead>
      <tbody>
        <tr v-for="o in operations" :key="'operations-op-'+operation_id(o.key)">
          <td>{{ o.index }}</td>
          <td :title="o.block">
            <a @click="$root.route('block', [['block', o.block]])" class="btn btn-link">{{ short_hash(o.block) }} ({{ o.key.index }})</a>
          </td>
          <td>{{ o.key.height }}</td>
          <td>
            <a @click="$root.route('operation', [['height', o.previousOperation.height.toString()], ['index', o.previousOperation.index.toString()]])" class="btn btn-link" v-if="o.previousOperation">{{ operation_id(o.previousOperation) }}</a>
            <span v-else>--</span>
          </td>
          <td>
            <button class="btn btn-link btn-sm" data-bs-toggle="modal" :data-bs-target="'#'+operation_id(o.key)+'-modal'" @click="json_load(operation_id(o.key)+'-json', o)">
              <i class="bi bi-braces"></i>
            </button>
            <div :id="operation_id(o.key)+'-modal'" class="modal fade">
              <div class="modal-dialog modal-xl">
                <div class="modal-content">
                  <div class="modal-header">
                    <h5 class="modal-title">Operation</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                  </div>
                  <div class="modal-body">
                    <div :id="operation_id(o.key)+'-json'" style="overflow-x: auto">
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
