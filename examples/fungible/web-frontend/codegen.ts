import { CodegenConfig } from '@graphql-codegen/cli';


const port = 8080;
const chainId = "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65";
const appId = "4855b92a65ca13ccf487f1a8f0225b49d9423c03be41996b582d96b5d1df28b2e97939c7631f353e08f6308f702abe2f1b4dd2c7b6358b22cf39bf331b76446fe476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000000000000"


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