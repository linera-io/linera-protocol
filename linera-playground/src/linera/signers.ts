import * as linera from '@linera/client';
import type { Signer } from '@linera/client';
import { Signer as MetaMaskSigner } from '@linera/metamask';

/** A signer together with the metadata the client manager needs to use and cache it. */
export interface ActiveSigner {
  /** Cache key for the underlying key; changing it forces the client to be rebuilt. */
  id: string;
  signer: Signer;
  /** The owner address that signs blocks, or `null` for the read-only ephemeral key. */
  owner: string | null;
}

let ephemeralCount = 0;

/**
 * A throwaway key for read-only browsing. Reads work; mutations are rejected by
 * the validators because this owner owns nothing.
 */
export function ephemeralSigner(): ActiveSigner {
  ephemeralCount += 1;
  return {
    id: `ephemeral:${ephemeralCount}`,
    signer: linera.signer.PrivateKey.createRandom(),
    owner: null,
  };
}

/** Build a signer from a pasted private key (hex) or mnemonic phrase. */
export function privateKeySigner(secret: string): ActiveSigner {
  const trimmed = secret.trim();
  const signer = /\s/.test(trimmed)
    ? linera.signer.PrivateKey.fromMnemonic(trimmed)
    : new linera.signer.PrivateKey(trimmed);
  const owner = signer.address();
  return { id: owner.toLowerCase(), signer, owner };
}

/** A minimal view of the EIP-1193 provider MetaMask injects on `window`. */
interface EthereumProvider {
  request(args: { method: string; params?: unknown[] }): Promise<unknown>;
}

function getEthereum(): EthereumProvider {
  const ethereum = (window as unknown as { ethereum?: EthereumProvider }).ethereum;
  if (!ethereum) {
    throw new Error('MetaMask is not available');
  }
  return ethereum;
}

/**
 * Connect the MetaMask browser extension as the signer.
 *
 * Requests the `eth_accounts` permission first, which always opens MetaMask's
 * account picker — even when the site is already authorized — so connecting and
 * switching accounts is explicit rather than silent.
 */
export async function metaMaskSigner(): Promise<ActiveSigner> {
  const ethereum = getEthereum();
  await ethereum.request({
    method: 'wallet_requestPermissions',
    params: [{ eth_accounts: {} }],
  });
  const signer = new MetaMaskSigner();
  const owner = await signer.address();
  return { id: owner.toLowerCase(), signer, owner };
}
