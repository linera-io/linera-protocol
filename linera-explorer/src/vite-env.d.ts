/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_FORMATS_REGISTRY_CHAIN?: string
  readonly VITE_FORMATS_REGISTRY_APP_ID?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
