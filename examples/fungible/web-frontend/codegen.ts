import { CodegenConfig } from '@graphql-codegen/cli';


const port = 8080;
const chainId = "aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8";
const appId = "4855b92a65ca13ccf487f1a8f0225b49d9423c03be41996b582d96b5d1df28b2e97939c7631f353e08f6308f702abe2f1b4dd2c7b6358b22cf39bf331b76446faee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8010000000000000000000000"


const config: CodegenConfig = {
  schema: `http://localhost:${port}/chains/${chainId}/applications/${appId}`,
  // this assumes that all your source files are in a top-level `src/` directory - you might need to adjust this to your file structure
  documents: ['src/**/*.{ts,tsx}'],
  generates: {
    './src/qql/': {
      preset: 'client',
      plugins: [],
    }
  },
  ignoreNoDocuments: true,
};

export default config;