import JSONFormatter from 'json-formatter-js'
import { Scalars } from '../../gql/operations'
import { TransactionMetadata, IncomingBundle, Operation } from '../../gql/service'
import init, { short_crypto_hash, short_app_id } from "../../pkg/linera_explorer"
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
  await init()
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
