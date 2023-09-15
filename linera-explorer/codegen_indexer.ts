import type { CodegenConfig } from '@graphql-codegen/cli'

const config: CodegenConfig = {
  overwrite: true,
  schema: "../linera-indexer/graphql-client/gql/indexer_schema.graphql",
  documents: "../linera-indexer/graphql-client/gql/indexer_requests.graphql",
  generates: {
    "gql/indexer.d.ts": {
      plugins: ['typescript']
    },
  }
}

export default config;
