import type { CodegenConfig } from '@graphql-codegen/cli'

const config: CodegenConfig = {
  overwrite: true,
  schema: "../linera-service-graphql-client/gql/service_schema.graphql",
  documents: "../linera-service-graphql-client/gql/service_requests.graphql",
  generates: {
    "gql/service.d.ts": {
      plugins: ['typescript']
    },
  }
}
export default config
