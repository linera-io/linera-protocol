import { ethers } from "ethers";
import { Signer } from "@linera/client";

import type { MetaMaskInpageProvider } from "@metamask/providers";

declare global {
  interface Window {
    ethereum?: MetaMaskInpageProvider;
  }
}

/**
 * A signer implementation that uses the MetaMask browser extension for signing.
 * 
 * This class relies on the global `window.ethereum` object injected by MetaMask
 * and interacts with it using EIP-191-compliant requests. It provides a secure
 * mechanism for message signing through the user's MetaMask wallet.
 * 
 * ⚠️ WARNING: This signer requires MetaMask to be installed and unlocked in the browser.
 * It will throw errors if MetaMask is unavailable, the user rejects a request, or
 * if the requested signer is not among the connected accounts.
 * 
 * The `MetaMask` signer verifies that the connected account matches the specified
 * owner address before signing a message. All messages are encoded as hexadecimal
 * strings and signed using the `personal_sign` method.
 * 
 * Suitable for production use where MetaMask is the expected signer interface.
 */
export class MetaMask implements Signer {
  private provider: ethers.BrowserProvider;

  constructor() {
    if (typeof window === "undefined" || !window.ethereum) {
      throw new Error("MetaMask is not available");
    }
    this.provider = new ethers.BrowserProvider(window.ethereum!);
  }

  async sign(owner: string, value: Uint8Array): Promise<string> {
    if (!window.ethereum) {
      throw new Error("MetaMask is not available");
    }

    // Explicitly type the result and check for undefined
    const accounts = (await window.ethereum.request({
      method: "eth_requestAccounts",
    })) as string[] | undefined;

    if (!accounts || accounts.length === 0) {
      throw new Error("No MetaMask accounts connected");
    }

    const connected = accounts.find(
      (acc) => acc.toLowerCase() === owner.toLowerCase(),
    );
    if (!connected) {
      throw new Error(
        `MetaMask is not connected with the requested owner: ${owner}`,
      );
    }

    // Encode message as hex string
    const msgHex = `0x${uint8ArrayToHex(value)}`;
    try {
      const signature = (await window.ethereum.request({
        method: "personal_sign",
        params: [msgHex, owner],
      })) as string;

      if (!signature) {
        throw new Error("No signature returned");
      }

      return signature;
    } catch (err: any) {
      throw new Error(
        `MetaMask signature request failed: ${err?.message || err}`,
      );
    }
  }

  async containsKey(owner: string): Promise<boolean> {
    const accounts = await this.provider.send("eth_requestAccounts", []);
    return accounts.some(
      (acc: string) => acc.toLowerCase() === owner.toLowerCase(),
    );
  }

  /**
   * Returns the currently connected MetaMask account address.
   */
  async address(): Promise<string> {
    const signer = await this.provider.getSigner();
    let address = await signer.getAddress();
    return address;
  }
}

function uint8ArrayToHex(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b: number) => b.toString(16).padStart(2, '0'))
    .join('');
}
