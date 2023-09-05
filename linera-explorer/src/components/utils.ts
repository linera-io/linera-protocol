import JSONFormatter from 'json-formatter-js'
import { Scalars } from '../../gql/operations'
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

export async function set_test_config() {
  await init()
  config.global.mocks.sh = short_crypto_hash
  config.global.mocks.shapp = short_app_id
  config.global.mocks.json_load = json_load
  config.global.mocks.operation_id = operation_id
}
