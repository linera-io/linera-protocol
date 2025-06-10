import { Wallet, ethers } from "ethers";
import { Signer } from "@linera/client";

export class LocalSigner implements Signer {
  private wallet: Wallet;

  constructor(privateKeyHex: string) {
    this.wallet = new Wallet(privateKeyHex);
  }

  static fromMnemonic(mnemonic: string): LocalSigner {
    const wallet = ethers.Wallet.fromPhrase(mnemonic);
    return new LocalSigner(wallet.privateKey);
  }

  public address(): string {
    return this.wallet.address;
  }

  async sign(owner: string, value: Uint8Array): Promise<string> {
    if (
      typeof owner !== "string" ||
      !ethers.isAddress(owner) ||
      this.wallet.address.toLowerCase() !== owner.toLowerCase()
    ) {
      throw new Error("Invalid owner address");
    }
    // ethers expects a string or Bytes for EIP-191
    const signature = await this.wallet.signMessage(value);

    return signature;
  }

  async getPublicKey(owner: string): Promise<string> {
    if (
      typeof owner !== "string" ||
      !ethers.isAddress(owner) ||
      this.wallet.address.toLowerCase() !== owner.toLowerCase()
    ) {
      throw new Error("Invalid owner address");
    }
    return this.wallet.signingKey.publicKey;
  }

  async containsKey(owner: string): Promise<boolean> {
    // The owner for Linera's EIP-191 wallet is the wallet address.
    if (typeof owner !== "string" || !ethers.isAddress(owner)) {
      throw new Error("Invalid owner address");
    }
    if (this.wallet.address.toLowerCase() !== owner.toLowerCase()) {
      return false; // The wallet does not contain the key for this owner
    }
    return true;
  }
}
