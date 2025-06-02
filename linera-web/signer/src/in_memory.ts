import { Wallet, ethers } from 'ethers';
import { IJsSigner } from '@linera/client';

export class EmbeddedEIP191Signer implements IJsSigner {
  private wallet: Wallet;

  constructor(privateKeyHex: string) {
    this.wallet = new Wallet(privateKeyHex);
  }

  static fromMnemonic(mnemonic: string): EmbeddedEIP191Signer {
    const wallet = ethers.Wallet.fromPhrase(mnemonic);
    return new EmbeddedEIP191Signer(wallet.privateKey);
  }

  public address(): string {
    return this.wallet.address;
  }

  async sign(owner: string, value: Uint8Array): Promise<string> {
    if (typeof owner !== 'string' || !ethers.isAddress(owner) || this.wallet.address.toLowerCase() !== owner.toLowerCase()) {
      throw new Error('Invalid owner address');
    }
    // ethers expects a string or Bytes for EIP-191
    const signature = await this.wallet.signMessage(value);

    return signature
  }

  /**
   * Returns the public key for the given owner.
   * In Linera's EIP-191 compatible wallet, the owner is the wallet address.
   * @param owner owner for which we want to get the public key
   * @returns Uncompressed public key in hex format.
   */
  async get_public_key(owner: string): Promise<string> {
    if (typeof owner !== 'string' || !ethers.isAddress(owner) || this.wallet.address.toLowerCase() !== owner.toLowerCase()) {
      throw new Error('Invalid owner address');
    }
    return this.wallet.signingKey.publicKey;
  }

  async contains_key(owner: string): Promise<boolean> {
    // The owner for Linera's EIP-191 wallet is the wallet address.
    if (typeof owner !== 'string' || !ethers.isAddress(owner)) {
      throw new Error('Invalid owner address');
    }
    if (this.wallet.address.toLowerCase() !== owner.toLowerCase()) {
      return false; // The wallet does not contain the key for this owner
    }
    return true;
  }
}
