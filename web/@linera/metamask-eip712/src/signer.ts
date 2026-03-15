import { Signer as MetaMaskSigner } from "@linera/metamask";

/**
 * A MetaMask signer that uses the EIP-712 signature scheme.
 *
 * Extends the base MetaMask signer with EIP-712 scheme identification.
 * When the Rust side queries `scheme()`, it receives `"Eip712Secp256k1"`,
 * which causes it to construct EIP-712 typed data and call `signTypedData()`
 * instead of the EIP-191 `sign()` path.
 */
export default class Signer extends MetaMaskSigner {
  async scheme(_owner: string): Promise<string> {
    return "Eip712Secp256k1";
  }
}
