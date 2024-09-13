{ crane, pkgs, rust-toolchain }:
((crane.mkLib pkgs).overrideToolchain rust-toolchain).buildPackage {
  pname = "linera";
  src = ./.;
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
  ];
  checkInputs = with pkgs; [
    # for native testing
    jq
    kubernetes-helm
    kind
    kubectl

    # for Wasm testing
    chromium
    chromedriver
    wasm-pack
  ];
  passthru = { inherit rust-toolchain; };
}
