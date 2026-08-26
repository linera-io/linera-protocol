{
  lib,
  rustPlatform,
  fetchFromGitHub,
  pkg-config,
  bzip2,
  openssl,
  zstd,
}:

rustPlatform.buildRustPackage (finalAttrs: {
  pname = "symbolicator";
  version = "26.3.0";

  src = fetchFromGitHub {
    owner = "getsentry";
    repo = "symbolicator";
    rev = finalAttrs.version;
    hash = "sha256-up23SMS/TWmgPm+VsWCEX/G8A8MgG9Vzay76tsHzo2M=";
    fetchSubmodules = true;
  };

  cargoHash = "sha256-GUWAG9mPPHUevA1IfFRpL9f93vUWb+/gaH0v+Dw9Rko=";

  nativeBuildInputs = [
    pkg-config
    rustPlatform.bindgenHook
  ];

  buildInputs = [
    bzip2
    openssl
    zstd
  ];

  env = {
    SYMBOLICATOR_GIT_VERSION = finalAttrs.src.rev;
    SYMBOLICATOR_RELEASE = finalAttrs.version;
    ZSTD_SYS_USE_PKG_CONFIG = true;
  };

  # tests require network access
  doCheck = false;

  meta = {
    description = "Native Symbolication as a Service";
    homepage = "https://getsentry.github.io/symbolicator/";
    changelog = "https://github.com/getsentry/symbolicator/blob/${finalAttrs.src.rev}/CHANGELOG.md";
    license = lib.licenses.mit;
    maintainers = [ ];
    mainProgram = "symbolicator";
  };
})