/**
 * Interface for signing and key management for Linera account owners.
 *
 * Implementations may back any subset of supported owner schemes:
 *   - `Address20` (EVM secp256k1, EIP-191 signatures)
 *   - `Address32` (Ed25519 over the 32-byte `CryptoHash` prehash)
 */
export interface Signer {
  /**
   * Signs `value` using the private key associated with `owner`.
   *
   * For `Address20` owners, the returned signature must follow EIP-191 (hex, `0x`-prefixed,
   * 65-byte r||s||v form). For `Address32` owners, the returned signature must be the raw
   * 64-byte Ed25519 signature (`r||s`) as a `0x`-prefixed hex string.
   *
   * @param owner - The account owner whose key signs.
   * @param value - The data to be signed.
   */
  sign(owner: string, value: Uint8Array): Promise<string>;

  /**
   * Returns the public key associated with `owner`, as a `0x`-prefixed hex string.
   *
   * For `Address20` owners, this is the uncompressed secp256k1 public key (65 bytes).
   * For `Address32` owners, this is the raw 32-byte Ed25519 public key.
   *
   * Required for `Address32` owners so the wasm bridge can construct
   * `AccountSignature::Ed25519 { signature, public_key }`.
   *
   * @param owner - The account owner to look up.
   */
  getPublicKey(owner: string): Promise<string>;

  /**
   * Checks whether the signer holds a key for `owner`.
   *
   * @param owner - The account owner to check.
   */
  containsKey(owner: string): Promise<boolean>;
}
