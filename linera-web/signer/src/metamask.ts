import { ethers } from "ethers";
import { Signer } from "@linera/client";

import type { MetaMaskInpageProvider } from "@metamask/providers";

declare global {
  interface Window {
    ethereum?: MetaMaskInpageProvider;
  }
}

export class MetaMaskEIP191Signer implements Signer {
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
    const msgHex = `0x${Buffer.from(value).toString("hex")}`;
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

  async getPublicKey(owner: string): Promise<string> {
    throw new Error(
      "MetaMask does not provide a way to retrieve the public key directly.",
    );
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
