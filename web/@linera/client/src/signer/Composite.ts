import type { Signer } from "./Signer.d.ts";

/**
 * A signer implementation that tries multiple signers in series.
 */
export default class Composite implements Signer {
  private signers: Signer[];

  constructor(...signers: Signer[]) {
    this.signers = signers;
  }

  async sign(owner: string, value: Uint8Array): Promise<string> {
    for (const signer of this.signers)
      if (await signer.containsKey(owner))
        return await signer.sign(owner, value);

    throw new Error(`no signer found for owner ${owner}`);
  }

  async containsKey(owner: string): Promise<boolean> {
    for (const signer of this.signers)
      if (await signer.containsKey(owner))
        return true;

    return false;
  }

  async signTypedData(owner: string, typedData: string): Promise<string> {
    for (const signer of this.signers)
      if (await signer.containsKey(owner))
        return await signer.signTypedData(owner, typedData);

    throw new Error(`no signer found for owner ${owner}`);
  }

  async scheme(owner: string): Promise<string> {
    for (const signer of this.signers)
      if (await signer.containsKey(owner))
        return await signer.scheme(owner);

    throw new Error(`no signer found for owner ${owner}`);
  }
}
