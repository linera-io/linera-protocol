import { CodegenConfig } from '@graphql-codegen/cli'
import 'dotenv/config'

/** 
  Make sure you set the correct chainId, app, and port in your .env file.

  This assumes that all your source files are in a top-level `src/` directory - you might need to adjust this to your file structure
*/
const app = process.env.APP
const port = process.env.PORT || 8000
const chainId = process.env.CHAIN_ID

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
