import JSONFormatter from 'json-formatter-js'
import { Scalars } from '../../gql/operations'
import { TransactionMetadata, IncomingBundle, Operation } from '../../gql/service'
import { initSync, short_crypto_hash, short_app_id } from "../../pkg/linera_explorer"
import { config } from '@vue/test-utils'

export function json_load(id: string, data: any) {
  let formatter = new JSONFormatter(data, Infinity)
  let elt = document.getElementById(id)
  elt!.appendChild(formatter.render())
}

export function operation_id(key: Scalars['OperationKey']['output']): string {
  return (short_crypto_hash(key.chain_id) + '-' + key.height + '-' + key.index)
}

async function set_test_config_aux() {
  // Use synchronous init for tests to avoid HTTP fetch.
  // Dynamic import to avoid bundling Node.js modules in production.
  const fs = await import('fs')
  const path = await import('path')
  const wasmPath = path.join(__dirname, '../../pkg/linera_explorer_bg.wasm')
  const wasmBytes = fs.readFileSync(wasmPath)
  initSync({ module: wasmBytes })

  config.global.mocks.short_hash = short_crypto_hash
  config.global.mocks.short_app_id = short_app_id
  config.global.mocks.json_load = json_load
  config.global.mocks.operation_id = operation_id
  return
}

function timeout(ms: number) : Promise<any> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function set_test_config() : Promise<void> {
  return set_test_config_aux().catch(async () => {
    await timeout(2000)
    await set_test_config_aux()
  })
}

// BigInt-safe display: converts BigInts to strings for display and JSON.stringify.
export function displayValue(v: any): string {
  if (typeof v === 'bigint') return v.toString()
  if (typeof v === 'object' && v !== null) {
    return JSON.stringify(v, (_k, val) => typeof val === 'bigint' ? val.toString() : val)
  }
  return String(v)
}

// Format a Linera timestamp (microseconds since epoch) as a UTC string.
export function formatTimestamp(ts: any): string {
  const n = Number(typeof ts === 'bigint' ? ts.toString() : ts)
  if (isNaN(n)) return String(ts)
  return new Date(n / 1000).toISOString().replace('T', ' ').replace('.000Z', ' UTC')
}

// Copy text to clipboard with visual feedback on the clicked element.
export function copyToClipboard(text: string, event?: MouseEvent) {
  navigator.clipboard.writeText(text).then(() => {
    if (event) {
      const el = event.currentTarget as HTMLElement
      const icon = el.querySelector('.bi-clipboard')
      if (icon) {
        icon.classList.remove('bi-clipboard')
        icon.classList.add('bi-check2')
        setTimeout(() => {
          icon.classList.remove('bi-check2')
          icon.classList.add('bi-clipboard')
        }, 1500)
      }
    }
  })
}

// Extract operations from transaction metadata
export function getOperations(transactionMetadata: TransactionMetadata[]): Operation[] {
  return transactionMetadata
    .filter(tx => tx.transactionType === "ExecuteOperation" && tx.operation)
    .map(tx => tx.operation!)
}

// Extract incoming bundles from transaction metadata  
export function getIncomingBundles(transactionMetadata: TransactionMetadata[]): IncomingBundle[] {
  return transactionMetadata
    .filter(tx => tx.transactionType === "ReceiveMessages" && tx.incomingBundle)
    .map(tx => tx.incomingBundle!)
}
