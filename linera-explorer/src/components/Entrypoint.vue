<script lang="ts">
import { query as rust_query, empty_response as rust_empty } from '../../pkg/linera_explorer'
import { defineComponent, PropType } from "vue"
import { IntrospectionField } from '../types'
import Json from './Json.vue'
import InputType from './InputType.vue'
import OutputType from './OutputType.vue'

export default defineComponent({
  props: {
    entry: {type: Object as PropType<IntrospectionField>, required: true},
    link: {type: String, required: true},
    kind: {type: String, required: true},
  },
  data() {
    return {
      result: undefined,
      errors: undefined,
    }
  },
  methods: {
    query(args: any, kind: string) {
      rust_query(this, args, kind)
    },
    empty_response(t: any) : any {
      return rust_empty(t)
    }
  }
})
</script>

<template>
  <div>
    <li class="list-group-item d-flex justify-content-between" data-bs-toggle="collapse" :data-bs-target="'#query-collapse-'+entry.name">
      <span>{{ entry.name }}</span>
      <i class="bi bi-caret-down-fill"></i>
    </li>
    <div class="collapse" :id="'query-collapse-'+entry.name">
      <!-- INPUT -->
      <div class="card card-body" v-if="entry.args.length!=0">
        <div class="card-title">INPUT</div>
        <div v-for="a in entry.args" :key="'entrypoint-'+a.name">
          <div class="form-label" :title="a.description">{{ a.name }}{{ a.type.kind=='NON_NULL' ? ' (*)' : '' }}</div>
          <InputType :elt="a.type"/>
        </div>
      </div>
      <!-- OUTPUT -->
      <div class="card card-body" v-if="!empty_response(entry.type)">
        <div class="card-title">OUTPUT</div>
        <OutputType :elt="entry.type" :depth="0"/>
      </div>
      <div class="text-center">
        <button class="btn btn-primary m-2" @click="query(entry, kind)">SEND</button>
      </div>
      <!-- RESULT -->
      <div class="card card-body" v-if="result">
        <div class="card-title">RESULT</div>
        <Json :data="result"/>
      </div>
      <!-- ERRORS -->
      <div class="card card-body" v-if="errors">
        <div class="card-title">ERRORS</div>
        <Json :data="errors"/>
      </div>
    </div>
  </div>
</template>
