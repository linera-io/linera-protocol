{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    systems.url = "github:nix-systems/default";

    # Rust
    rust-overlay.url = "github:oxalica/rust-overlay";
    crane.url = "github:ipetkov/crane";
    nixup.url = "github:Twey/nixup";

    # Dev tools
    treefmt-nix.url = "github:numtide/treefmt-nix";
  };

  outputs = inputs: inputs.flake-parts.lib.mkFlake { inherit inputs; } {
    systems = import inputs.systems;
    imports = [
      inputs.treefmt-nix.flakeModule
    ];
    perSystem = { config, self', inputs', pkgs, lib, system, ... }: {
      _module.args.pkgs = import inputs.nixpkgs {
        inherit system;
        overlays = [ (import inputs.rust-overlay) ];
      };

      packages = let
        docker-opts = {
          name = "linera-dev-container";
          tag = "latest";
          drv = linera;
        };
        rust-stable = pkgs.rust-bin.fromRustupToolchainFile
          ./toolchains/stable/rust-toolchain.toml;
        linera = pkgs.callPackage ./. {
          inherit (inputs) crane;
          rust-toolchain = rust-stable // pkgs.callPackage inputs.nixup { } {
            default = rust-stable;
            nightly = pkgs.rust-bin.fromRustupToolchainFile
              ./toolchains/nightly/rust-toolchain.toml;
          };
        };
      in {
        inherit linera;
        default = linera;
        container = pkgs.dockerTools.buildNixShellImage docker-opts;
        container-stream = pkgs.dockerTools.streamNixShellImage docker-opts;
      };

      # Rust dev environment
      devShells = let
        linera = self'.packages.linera;
        container = self'.packages.container;
      in {
        default = pkgs.mkShell {
          inputsFrom = [
            config.treefmt.build.devShell
            linera
          ];
          shellHook = ''
            export PATH=$PWD/target/debug:$PATH
            export RUST_SRC_PATH="${linera.RUST_SRC_PATH}"
            export LIBCLANG_PATH="${linera.LIBCLANG_PATH}"
            export ROCKSDB_LIB_DIR="${linera.ROCKSDB_LIB_DIR}"
          '';
          nativeBuildInputs = [ pkgs.rust-analyzer ];
        };

        docker = pkgs.mkShell {
          shellHook = ''
            docker images -q ${container.imageName}:${container.imageTag} || ${self'.packages.container-stream} | docker load
            exec docker run --user $USER_ID:$GROUP_ID -v .:/build -it ${container.imageName}:${container.imageTag}
          '';
        };
      };

      # Add your auto-formatters here.
      # cf. https://numtide.github.io/treefmt/
      treefmt.config = {
        projectRootFile = "flake.nix";
        programs.nixpkgs-fmt.enable = true;
      };
    };
  };
}
