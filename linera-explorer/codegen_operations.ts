import type { CodegenConfig } from '@graphql-codegen/cli'

const config: CodegenConfig = {
  overwrite: true,
  schema: "../linera-indexer/graphql-client/gql/operations_schema.graphql",
  documents: "../linera-indexer/graphql-client/gql/operations_requests.graphql",
  generates: {
    "gql/operations.d.ts": {
      plugins: ['typescript']
    },
  }
}
export default config
