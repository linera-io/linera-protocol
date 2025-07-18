{ crane, pkgs, rust-toolchain, libclang, rocksdb, git, system, nix-gitignore }:
((crane.mkLib pkgs).overrideToolchain rust-toolchain).buildPackage {
  pname = "linera";
  src = nix-gitignore.gitignoreSource [] (builtins.path {
    name = "source";
    path = ./.;
  });
  cargoExtraArgs = "-p linera-service";
  nativeBuildInputs = with pkgs; [
    clang
    pkg-config
    rocksdb
    protobufc
  ];
  buildInputs = with pkgs; [
    clang.cc.lib
    libiconv
    nodejs
    openssl
    protobuf
    git
    wasm-bindgen-cli
    pnpm
  ];
  checkInputs = with pkgs; [
    jq
    kubernetes-helm
    kind
    kubectl
  ] ++ lib.optionals (system != "arm64-apple-darwin") [
    # for Wasm testing
    # Chromium doesn't build on macOS so we can't run these tests there
    chromium
    chromedriver
  ];
  doCheck = false;
  passthru = { inherit rust-toolchain; };
  RUST_SRC_PATH = rust-toolchain.availableComponents.rust-src;
  LIBCLANG_PATH = "${libclang.lib}/lib";
  ROCKSDB_LIB_DIR = "${rocksdb}/lib";
}
