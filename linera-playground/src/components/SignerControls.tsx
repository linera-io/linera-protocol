import { useState } from 'react';

interface SignerControlsProps {
  owner: string | null;
  error: string | null;
  onUseKey: (secret: string) => void;
  onUseMetaMask: () => void;
  onDisconnect: () => void;
}

/** Connect a signer (pasted private key or MetaMask) for mutations, or show the
 *  read-only state when none is connected. */
export function SignerControls({
  owner,
  error,
  onUseKey,
  onUseMetaMask,
  onDisconnect,
}: SignerControlsProps) {
  const [secret, setSecret] = useState('');

  return (
    <div className="signer">
      {owner ? (
        <label className="field">
          <span>Signer</span>
          <div className="signer__row">
            <code className="signer__owner">{owner}</code>
            <button type="button" onClick={onDisconnect}>
              Disconnect
            </button>
          </div>
        </label>
      ) : (
        <label className="field">
          <span>
            Signer — <span className="signer__readonly">read-only</span>
          </span>
          <div className="signer__row">
            <input
              type="password"
              value={secret}
              placeholder="private key or mnemonic (for mutations)"
              onChange={(event) => setSecret(event.target.value)}
            />
            <button
              type="button"
              disabled={!secret.trim()}
              onClick={() => {
                onUseKey(secret);
                setSecret('');
              }}
            >
              Use key
            </button>
            <button type="button" onClick={onUseMetaMask}>
              Connect MetaMask
            </button>
          </div>
        </label>
      )}
      {error && <span className="signer__error">{error}</span>}
    </div>
  );
}
