<script setup lang="ts">
import { ApplicationOverview } from '../../gql/service'

defineProps<{apps: ApplicationOverview[]}>()

function safeStringify(obj: any): string {
  return JSON.stringify(obj, (_key, value) =>
    typeof value === 'bigint' ? value.toString() : value
  )
}
</script>

<template>
  <div v-if="apps.length==0" class="text-center">
    No applications for this chain
  </div>
  <div v-else>
    <table class="table">
      <thead>
        <th>Id</th>
        <th>GraphiQL</th>
        <th>JSON</th>
      </thead>
      <tbody>
        <tr v-for="a in apps" :key="'application-'+a.id">
          <td :title="a.id">
            <a target="_blank" class="btn btn-link btn-sm" @click="$root.route('application', [['app', safeStringify(a)]])">
              {{ short_app_id(a.id) }}
            </a>
          </td>
          <td :title="a.link">
            <button class="btn btn-link btn-sm" data-bs-toggle="modal" :data-bs-target="'#'+a.id+'-graphiql-modal'">
              <i class="bi bi-bounding-box-circles"></i>
            </button>
            <div :id="a.id+'-graphiql-modal'" class="modal fade">
              <div class="modal-dialog modal-fullscreen">
                <div class="modal-content">
                  <div class="modal-header">
                    <h5 class="modal-title">GraphiQL</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                  </div>
                  <div class="modal-body p-0">
                    <iframe :src="a.link" style="width: 100%; height: 100%; border: none;"></iframe>
                  </div>
                </div>
              </div>
            </div>
          </td>
          <td>
            <button class="btn btn-link btn-sm" data-bs-toggle="modal" :data-bs-target="'#'+a.id+'-modal'" @click="json_load(a.id+'-json', a)">
              <i class="bi bi-braces"></i>
            </button>
            <div :id="a.id+'-modal'" class="modal fade">
              <div class="modal-dialog modal-xl">
                <div class="modal-content">
                  <div class="modal-header">
                    <h5 class="modal-title">Application</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                  </div>
                  <div class="modal-body">
                    <div :id="a.id+'-json'" style="overflow-x: auto">
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
