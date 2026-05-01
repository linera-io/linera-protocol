<script setup lang="ts">
import { ref, watch, getCurrentInstance } from 'vue'
import { ApplicationOverview } from '../../gql/service'

const props = defineProps<{apps: ApplicationOverview[]}>()

function safeStringify(obj: any): string {
  return JSON.stringify(obj, (_key, value) =>
    typeof value === 'bigint' ? value.toString() : value
  )
}

// Per-application formats lookup state. Keyed by application id; value is null
// until we resolve, then either an object (formats available) or false (not
// available — no registry, no entry, etc.).
const formatsState = ref<Record<string, any | false | null>>({})

async function refreshFormats() {
  const root: any = getCurrentInstance()?.proxy?.$root
  if (!root || typeof root.fetch_user_app_formats !== 'function') return
  if (!root.config?.formats_registry_chain || !root.config?.formats_registry_app_id) {
    // Mark all entries as "not available" (formats registry not configured).
    const next: Record<string, any | false | null> = {}
    for (const a of props.apps) next[a.id] = false
    formatsState.value = next
    return
  }
  // Fetch in parallel; fill the map as each resolves so the UI can update
  // incrementally.
  await Promise.all(props.apps.map(async (a) => {
    try {
      const value = await root.fetch_user_app_formats(a.id)
      formatsState.value = { ...formatsState.value, [a.id]: value ?? false }
    } catch (e) {
      formatsState.value = { ...formatsState.value, [a.id]: false }
    }
  }))
}

watch(() => props.apps.map(a => a.id).join('|'), refreshFormats, { immediate: true })
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
        <th>Formats</th>
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
          <td>
            <template v-if="formatsState[a.id] === null">
              <span class="spinner-border spinner-border-sm text-muted"></span>
            </template>
            <template v-else-if="formatsState[a.id] === false">
              <span class="text-muted" :title="($root.config?.formats_registry_chain && $root.config?.formats_registry_app_id) ? 'No formats registered for this module' : 'Set the formats registry chain and app id in the navbar'">
                <i class="bi bi-x-circle"></i>
              </span>
            </template>
            <template v-else>
              <button class="btn btn-link btn-sm" data-bs-toggle="modal" :data-bs-target="'#'+a.id+'-formats-modal'" @click="json_load(a.id+'-formats-json', formatsState[a.id])" title="Show registered formats">
                <i class="bi bi-check2-circle text-success"></i>
              </button>
              <div :id="a.id+'-formats-modal'" class="modal fade">
                <div class="modal-dialog modal-xl">
                  <div class="modal-content">
                    <div class="modal-header">
                      <h5 class="modal-title">Formats — {{ short_app_id(a.id) }}</h5>
                      <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                      <div :id="a.id+'-formats-json'" style="overflow-x: auto"></div>
                    </div>
                  </div>
                </div>
              </div>
            </template>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>
