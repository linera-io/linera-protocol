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
        cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
        nonRustDeps = with pkgs; [
        ];
        linera = pkgs.callPackage ./. { inherit (inputs) crane; };
      in {
        _module.args.pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [ (import inputs.rust-overlay) ];
        };

        packages.default = linera;

        # Rust dev environment
        devShells.default = pkgs.mkShell {
          inputsFrom = [
            config.treefmt.build.devShell
            linera
          ];
          shellHook = ''
            # For rust-analyzer 'hover' tooltips to work.
            export RUST_SRC_PATH=${linera.rust-toolchain.availableComponents.rust-src}
            export LIBCLANG_PATH=${pkgs.libclang.lib}/lib
            export PATH=$PWD/target/debug:~/.cargo/bin:$PATH
            export ROCKSDB_LIB_DIR=${pkgs.rocksdb}/lib
          '';
          nativeBuildInputs = with pkgs; [
            rust-analyzer
          ];
        };

        devShells.lint = pkgs.mkShell {
          nativeBuildInputs = [
            (pkgs.rust-bin.fromRustupToolchainFile
              ./toolchains/lint/rust-toolchain.toml)
          ];
        };

        # Add your auto-formatters here.
        # cf. https://numtide.github.io/treefmt/
        treefmt.config = {
          projectRootFile = "flake.nix";
          programs = {
            nixpkgs-fmt.enable = true;
            rustfmt.enable = true;
          };
        };
      };
  };
}
