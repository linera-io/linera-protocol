<script setup lang="ts">
import { append_input as append, remove_input as remove } from '../../pkg/linera_explorer'
import { IntrospectionFull } from '../types'
import Json from './Json.vue'

const props = defineProps<{elt: IntrospectionFull, offset?: boolean}>()

function append_input() { append(props) }
function remove_input(i: number) { remove(props, i) }
</script>

<template>
  <div v-if="!elt">TYPE NOT FOUND</div>
  <input v-else-if="elt.kind=='SCALAR' && (elt.name=='Int' || elt.name=='Float')" class="form-control" type="number" v-model.number="elt._input">
  <select v-else-if="elt.kind=='SCALAR' && elt.name=='Boolean'" class="form-select" v-model="elt._input">
    <option :value="true" selected>true</option>
    <option :value="false">false</option>
  </select>
  <input v-else-if="elt.kind=='SCALAR'" class="form-control" type="text" v-model="elt._input">
  <InputType v-else-if="elt.kind=='NON_NULL'" :elt="elt.ofType!" :offset="offset"/>
  <div v-else-if="elt.kind=='INPUT_OBJECT'" class="row">
    <div v-for="f in elt.inputFields" :class="offset ? 'col-11 offset-1' : 'col-12'">
      <div class="form-label" :title="f.description">{{ f.name }}{{ f.type.kind=='NON_NULL' ? ' (*)' : '' }}</div>
      <InputType :elt="f.type" :offset="true"/>
    </div>
  </div>
  <div v-else-if="elt.kind=='LIST'">
    <div v-for="(e, i) in elt._input" class="row">
      <span class="col-1 d-grid gap-2" v-if="e!==undefined">
        <button class="btn btn-outline-secondary" @click="remove_input(i)">
          <i class="bi bi-dash-circle-fill"></i>
        </button>
      </span>
      <span class="col-11" v-if="e!==undefined">
        <InputType :elt="e" :offset="false"/>
      </span>
    </div>
    <div class="row">
      <span class="col-1 d-grid gap-2">
        <button class="btn btn-outline-secondary" @click="append_input()">
          <i class="bi bi-plus-circle-fill"></i>
        </button>
      </span>
    </div>
  </div>
  <select v-else-if="elt.kind=='ENUM'" class="form-select" v-model="elt._input">
    <option v-for="e in elt.enumValues" :value="e.name" selected>{{ e.name }}</option>
  </select>
  <div v-else>
    <Json :data="elt"/>
  </div>
</template>
