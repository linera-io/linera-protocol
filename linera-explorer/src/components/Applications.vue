<script setup lang="ts">
import { ApplicationOverview } from '../../gql/service'

defineProps<{apps: ApplicationOverview[]}>()
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
        <tr v-for="a in apps" :key="'application-'+a.address">
          <td :title="a.address">
            <a target="_blank" class="btn btn-link btn-sm" @click="$root.route('application', [['app', JSON.stringify(a)]])">
              {{ short_app_id(a.address) }}
            </a>
          </td>
          <td :title="a.link">
            <a :href="a.link" target="_blank" class="btn btn-link btn-sm">
              <i class="bi bi-bounding-box-circles"></i>
            </a>
          </td>
          <td>
            <button class="btn btn-link btn-sm" data-bs-toggle="modal" :data-bs-target="'#'+a.address+'-modal'" @click="json_load(a.address+'-json', a)">
              <i class="bi bi-braces"></i>
            </button>
            <div :id="a.address+'-modal'" class="modal fade">
              <div class="modal-dialog modal-xl">
                <div class="modal-content">
                  <div class="modal-header">
                    <h5 class="modal-title">Application</h5>
                    <button type="button" class="btn-close" data-bs-digsmiss="modal"></button>
                  </div>
                  <div class="modal-body">
                    <div :id="a.address+'-json'" style="overflow-x: auto">
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
