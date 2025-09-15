/**
 * Interface for signing and key management compatible with Ethereum (EVM) addresses.
 */
export interface Signer {
  /**
   * Signs a given value using the private key associated with the specified EVM address.
   * The signing process must follow the EIP-191 standard.
   *
   * @param owner - The EVM address whose private key will be used to sign the value.
   * @param value - The data to be signed, as a `Uint8Array`.
   * @returns A promise that resolves to the EIP-191-compatible signature in hexadecimal string format.
   */
  sign(owner: string, value: Uint8Array): Promise<string>;

  /**
   * Checks whether the instance holds a key whose associated address matches the given EVM address.
   *
   * @param owner - The EVM address to check for.
   * @returns A promise that resolves to `true` if the key exists and matches the given address, otherwise `false`.
   */
  containsKey(owner: string): Promise<boolean>;
}
