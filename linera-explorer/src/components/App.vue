<script lang="ts">
import { data, save_config, route } from '../../pkg/linera_explorer'
import Block from './Block.vue'
import Blocks from './Blocks.vue'
import Chain from './Chain.vue'
import Applications from './Applications.vue'
import Application from './Application.vue'
import Operations from './Operations.vue'
import Operation from './Operation.vue'
import Plugin from './Plugin.vue'

export default {
  data() { return data() },
  methods: {
    save_config() { save_config(this) },
    route(name?: string, args?: [string, string][]) { route(this, name, args) }
  },
  components: {
    Block,
    Blocks,
    Chain,
    Applications,
    Application,
    Operations,
    Operation,
    Plugin
  },
}
</script>

<template>
  <div>
    <nav class="navbar navbar-expand-sm m-2">
      <div class="container-fluid">
        <a class="navbar-brand" type="button" @click="route('')">Linera Explorer</a>
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarSupportedContent" aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
          <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarSupportedContent">
          <ul class="navbar-nav me-auto">
            <li class="nav-item">
              <a class="nav-link" :class="page.blocks ? 'active' : ''" @click="route('blocks')" role="button">Blocks</a>
            </li>
            <li class="nav-item">
              <a class="nav-link" :class="page.applications ? 'active' : ''" @click="route('applications')" role="button">Applications</a>
            </li>
            <li class="nav-item" v-if="plugins.includes('operations')">
              <a class="nav-link" :class="page.operations ? 'active' : ''" @click="route('operations')" role="button">Operations</a>
            </li>
            <li v-if="plugins.length!=0" class="nav-item dropdown">
              <a class="nav-link dropdown-toggle" role="button" data-bs-toggle="dropdown">
                Indexer
              </a>
              <ul class="dropdown-menu">
                <li v-for="plugin in plugins"><a class="dropdown-item" @click="route('plugin', [['plugin', plugin]])">{{ plugin }}</a></li>
              </ul>
            </li>
          </ul>
          <ul class="navbar-nav ms-auto">
            <li class="nav-item mx-2">
              <select @change="route(undefined, [['chain', ($event.target! as HTMLButtonElement).value]])" class="form-select">
                <option v-for="c in chains" :value="c" :selected="c==chain" :key="'chain-'+c">{{ short_hash(c) }}</option>
              </select>
            </li>
            <li class="nav-item mx-2">
              <div class="input-group">
                <span class="input-group-text">node</span>
                <input v-model="config.node" class="form-control" @change="save_config" style="width:190px">
                <span class="input-group-text">indexer</span>
                <input v-model="config.indexer" class="form-control" @change="save_config" style="width:190px">
              </div>
            </li>
            <li class="nav-item mx-2">
              <div class="form-control form-check form-switch">
                <input class="form-check-input" type="checkbox" v-model="config.tls" id="tls-check" @change="save_config">
                <label class="form-check-label" for="tls-check">TLS/SSL</label>
              </div>
            </li>
            <li class="nav-item mx-2">
              <button @click="route()" class="btn btn-primary">
                <i class="bi bi-arrow-clockwise"></i>
              </button>
            </li>
          </ul>
        </div>
      </div>
    </nav>
    <div class="container pb-5">
      <div v-if="page=='unloaded'">
        <div class="text-center m-5 p-5">
          <span class="spinner-border">
          </span>
        </div>
      </div>

      <div v-else-if="page.home">
        <Chain title="Chain" :chain="page.home.chain" />
        <Block v-if="page.home.blocks.length!=0" :block="page.home.blocks[0]" title="Head"/>
        <div class="card">
          <div class="card-header">Blocks</div>
          <div class="card-body">
            <Blocks :blocks="page.home.blocks"/>
          </div>
        </div>
        <div class="card">
          <div class="card-header">Applications</div>
          <div class="card-body">
            <Applications :apps="page.home.apps"/>
          </div>
        </div>
      </div>

      <div v-else-if="page.blocks">
        <Blocks :blocks="page.blocks"/>
      </div>

      <div v-else-if="page.block">
        <Block :block="page.block" title="Block"/>
      </div>

      <div v-else-if="page.applications">
        <Applications :apps="page.applications"/>
      </div>

      <div v-else-if="page.application">
        <Application :app="page.application"/>
      </div>

      <div v-else-if="page.operations">
        <Operations :operations="page.operations"/>
      </div>

      <div v-else-if="page.operation">
        <Operation :op="page.operation" :id="operation_id(page.operation.key)" :index="page.operation.index"/>
      </div>

      <div v-else-if="page.plugin">
        <Plugin :plugin="page.plugin"/>
      </div>

      <div v-else-if="page.error">
        <div class="m-5 p-5 text-center">
          {{ page.error }}
        </div>
      </div>

      <div v-else>
        Page not found
      </div>

    </div>
  </div>
</template>
