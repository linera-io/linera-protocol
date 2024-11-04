{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    systems.url = "github:nix-systems/default";

    # Rust
    rust-overlay.url = "github:oxalica/rust-overlay";
    crane.url = "github:ipetkov/crane";

    # Dev tools
    treefmt-nix.url = "github:numtide/treefmt-nix";
  };

  outputs = inputs: inputs.flake-parts.lib.mkFlake { inherit inputs; } {
    systems = import inputs.systems;
    imports = [
      inputs.treefmt-nix.flakeModule
    ];
    perSystem = { config, self', inputs', pkgs, lib, system, ... }:
      let
        linera = rust-toolchain: pkgs.callPackage ./. {
          inherit (inputs) crane;
          inherit rust-toolchain;
        };
        rust-stable = (pkgs.rust-bin.fromRustupToolchainFile
          ./toolchains/stable/rust-toolchain.toml);
        rust-nightly = (pkgs.rust-bin.fromRustupToolchainFile
          ./toolchains/nightly/rust-toolchain.toml);
        devShell = rust-toolchain: let linera' = linera rust-toolchain; in pkgs.mkShell {
          inputsFrom = [
            config.treefmt.build.devShell
            linera'
          ];
          shellHook = ''
            # For rust-analyzer 'hover' tooltips to work.
            export PATH=$PWD/target/debug:~/.cargo/bin:$PATH
            export RUST_SRC_PATH="${linera'.RUST_SRC_PATH}"
            export LIBCLANG_PATH="${linera'.LIBCLANG_PATH}"
            export ROCKSDB_LIB_DIR="${linera'.ROCKSDB_LIB_DIR}";
          '';
          nativeBuildInputs = with pkgs; [
            rust-analyzer
          ];
        };
      in {
        _module.args.pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [ (import inputs.rust-overlay) ];
        };

        packages.default = linera rust-stable;

        # Rust dev environment
        devShells.default = devShell rust-stable;
        devShells.nightly = devShell rust-nightly;

        # Add your auto-formatters here.
        # cf. https://numtide.github.io/treefmt/
        treefmt.config = {
          projectRootFile = "flake.nix";
          programs = {
            nixpkgs-fmt.enable = true;
          };
        };
      };
  };
}
