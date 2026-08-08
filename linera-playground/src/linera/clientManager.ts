import * as linera from '@linera/client';
import type { ActiveSigner } from './signers';

/** The chain and application a query is run against. */
export interface QueryTarget {
  faucetUrl: string;
  chainId: string;
  appId: string;
}

/** A GraphQL request envelope as the application's service expects it. */
export interface GraphQLRequest {
  query: string;
  variables?: Record<string, unknown> | null;
  operationName?: string | null;
}

function log(message: string, ...args: unknown[]): void {
  console.info(`[linera-playground] ${message}`, ...args);
}

/**
 * Owns the `@linera/client` WASM objects and their lifecycle.
 *
 * A single `Client` is kept per (faucet URL, signer) pair, with `Chain` and
 * `Application` handles memoized on top of it. Changing the faucet URL or the
 * signer tears everything down — freeing the WASM handles and disposing the
 * client so its wallet storage lock is released — before a new client is built.
 */
export class LineraClientManager {
  private signer: ActiveSigner;
  private clientKey: string | null = null;
  private client: linera.Client | null = null;
  private readonly chains = new Map<string, linera.Chain>();
  private readonly apps = new Map<string, linera.Application>();

  constructor(signer: ActiveSigner) {
    this.signer = signer;
  }

  get owner(): string | null {
    return this.signer.owner;
  }

  /** Swap the active signer, discarding any client and handles bound to the old one. */
  async setSigner(signer: ActiveSigner): Promise<void> {
    this.signer = signer;
    await this.teardown();
  }

  /** Synchronize the target chain, then run a GraphQL request against the application. */
  async query(target: QueryTarget, request: GraphQLRequest): Promise<unknown> {
    const client = await this.ensureClient(target.faucetUrl);
    const chain = await this.ensureChain(client, target.chainId);

    log(`chain ${target.chainId}: synchronizing from validators…`);
    const start = performance.now();
    await chain.synchronize();
    log(`chain ${target.chainId}: synchronized in ${Math.round(performance.now() - start)}ms`);

    const app = await this.ensureApp(chain, target.chainId, target.appId);
    log(`application ${target.appId}: running ${request.operationName ?? 'operation'}`);
    const response = JSON.parse(await app.query(JSON.stringify(request)));
    log(`application ${target.appId}: query complete`);
    return response;
  }

  private async ensureClient(faucetUrl: string): Promise<linera.Client> {
    const key = `${faucetUrl}|${this.signer.id}`;
    if (this.client && this.clientKey === key) {
      return this.client;
    }
    await this.teardown();
    log(`building client: faucet=${faucetUrl}, signer=${this.signer.id}`);
    const faucet = await new linera.Faucet(faucetUrl);
    const wallet = await faucet.createWallet();
    this.client = await new linera.Client(wallet, this.signer.signer);
    this.clientKey = key;
    log('client ready');
    return this.client;
  }

  private async ensureChain(client: linera.Client, chainId: string): Promise<linera.Chain> {
    const key = `${chainId}|${this.signer.owner ?? ''}`;
    let chain = this.chains.get(key);
    if (!chain) {
      log(`chain ${chainId}: connecting (owner=${this.signer.owner ?? 'read-only'})`);
      chain = this.signer.owner
        ? await client.chain(chainId, { owner: this.signer.owner })
        : await client.chain(chainId);
      this.chains.set(key, chain);
    }
    return chain;
  }

  private async ensureApp(
    chain: linera.Chain,
    chainId: string,
    appId: string,
  ): Promise<linera.Application> {
    const key = `${chainId}|${appId}`;
    let app = this.apps.get(key);
    if (!app) {
      app = await chain.application(appId);
      this.apps.set(key, app);
    }
    return app;
  }

  private async teardown(): Promise<void> {
    for (const app of this.apps.values()) app.free();
    this.apps.clear();
    for (const chain of this.chains.values()) chain.free();
    this.chains.clear();
    const client = this.client;
    this.client = null;
    this.clientKey = null;
    if (client) {
      log('disposing previous client');
      try {
        await client.asyncDispose();
      } catch {
        // Best effort: a failed dispose must not block building the next client.
      }
    }
  }
}
