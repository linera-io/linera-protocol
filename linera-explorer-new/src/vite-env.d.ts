/// <reference types="vite/client" />

interface ImportMetaEnv {
  // usage: import.meta.env.VITE_API_BASE_URL
  readonly VITE_API_BASE_URL: string; 
  
  // Add other vars here
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
