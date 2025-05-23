import { CodegenConfig } from '@graphql-codegen/cli'
import 'dotenv/config'

/** 
  Make sure you set the correct chainId, app, and port in your .env file.

  This assumes that all your source files are in a top-level `src/` directory - you might need to adjust this to your file structure
*/
const app =
  import.meta.env.APP ||
  '3a59ba793926405c84b8db4e3a2cddaeb202fa1a914580bfad37055eea1b68e'
const port = import.meta.env.PORT || 8080
const chainId =
  import.meta.env.CHAIN_ID ||
  'b6f098ac296e4819f651be0d7a93f4bf3841a9c9a33b4b06a089fc614cc418d7'

const config: CodegenConfig = {
  schema: `http://localhost:${port}/chains/${chainId}/applications/${app}`,
  documents: ['src/**/*.{ts,tsx}'],
  generates: {
    './src/__generated__/': {
      preset: 'client',
      plugins: [],
      presetConfig: {
        gqlTagName: 'gql',
      },
    },
  },
  ignoreNoDocuments: true,
}

export default config
