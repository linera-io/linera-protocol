{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    systems.url = "github:nix-systems/default";
    rust-overlay.url = "github:oxalica/rust-overlay";

    # Dev tools
    treefmt-nix.url = "github:numtide/treefmt-nix";
  };

  outputs = inputs: inputs.flake-parts.lib.mkFlake { inherit inputs; } {
    systems = import inputs.systems;
    imports = [
      inputs.treefmt-nix.flakeModule
    ];
    perSystem = { config, self', pkgs, lib, system, rust-overlay, ... }:
      let
        cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
        nonRustDeps = with pkgs; [
          clang
          libclang.lib
          libiconv
          openssl
          protobuf
          pkg-config
          nodejs
        ];
        rustBuildToolchain = (pkgs.rust-bin.fromRustupToolchainFile
          ./toolchains/build/rust-toolchain.toml);
        rustLintToolchain = (pkgs.rust-bin.fromRustupToolchainFile
          ./toolchains/lint/rust-toolchain.toml);
      in {
        _module.args.pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [ (import inputs.rust-overlay) ];
        };

        # Rust dev environment
        devShells.default = pkgs.mkShell {
          inputsFrom = [
            config.treefmt.build.devShell
          ];
          shellHook = ''
            # For rust-analyzer 'hover' tooltips to work.
            export RUST_SRC_PATH=${rustBuildToolchain.availableComponents.rust-src}
            export LIBCLANG_PATH=${pkgs.libclang.lib}/lib
            export PATH=$PWD/target/debug:~/.cargo/bin:$PATH
          '';
          buildInputs = nonRustDeps;
          nativeBuildInputs = with pkgs; [
            rustBuildToolchain
            rust-analyzer
          ];
        };

        devShells.lint = pkgs.mkShell {
          nativeBuildInputs = [ rustLintToolchain ];
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
