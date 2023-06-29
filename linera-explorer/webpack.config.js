const path = require("path");
const CopyPlugin = require("copy-webpack-plugin");
const WasmPackPlugin = require("@wasm-tool/wasm-pack-plugin");
const dist = path.resolve(__dirname, "dist");

module.exports = {
  mode: "production",
  entry: { index: "./js/index.js" },
  output: { path: dist, filename: "[name].js" },
  devServer: { contentBase: dist },
  plugins: [
    new CopyPlugin({ patterns: [ { from: "index.html", to: "." } ]}),
    new WasmPackPlugin({ crateDirectory: __dirname }),
  ],
  experiments: { asyncWebAssembly: true },
  module: { rules: [ { test: /\.html$/i, loader: "html-loader" } ] },
};
