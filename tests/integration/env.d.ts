/// <reference types="@cloudflare/vitest-pool-workers" />

declare module 'cloudflare:test' {
  interface ProvidedEnv {
    BUCKET: R2Bucket;
    RPC_NAMESPACE: DurableObjectNamespace;
  }
}
