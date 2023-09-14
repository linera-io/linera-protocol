<script setup lang="ts">
import { IntrospectionFull } from '../types'
import Json from './Json.vue'

defineProps<{elt: IntrospectionFull, name?: string, depth: number}>()
</script>

<template>
  <div v-if="depth==0 && elt.kind=='SCALAR'"></div>
  <div v-else-if="elt.kind=='SCALAR' || elt.kind=='ENUM'" class="form-check">
    <input class="form-check-input" type="checkbox" v-model="elt._include">
    <label class="form-check-label">{{ name || elt.name }}</label>
  </div>
  <OutputType v-else-if="elt.kind=='NON_NULL' || elt.kind=='LIST'" :elt="elt.ofType!" :name="name || elt.name" :depth="depth"/>
  <div v-else-if="elt.kind=='OBJECT'">
    <div class="form-check" v-if="depth!=0">
      <input class="form-check-input" type="checkbox" v-model="elt._include">
      <label class="form-check-label">{{ name || elt.name }}</label>
    </div>
    <div class="row">
      <div v-for="f in elt.fields" :class="depth==0 ? 'col-12' : 'col-11 offset-1'">
        <OutputType :elt="f.type" :name="f.name" :depth="depth+1"/>
      </div>
    </div>
  </div>
  <div v-else>
    <Json :data="elt"/>
  </div>
</template>
