interface ConfigBarProps {
  faucetUrl: string;
  chainId: string;
  appId: string;
  onFaucetUrl: (value: string) => void;
  onChainId: (value: string) => void;
  onAppId: (value: string) => void;
  onApply: () => void;
}

/** The target inputs: faucet URL, chain ID and application ID, plus an Apply
 *  action that (re)loads the application's schema into the editor. */
export function ConfigBar({
  faucetUrl,
  chainId,
  appId,
  onFaucetUrl,
  onChainId,
  onAppId,
  onApply,
}: ConfigBarProps) {
  const ready = Boolean(faucetUrl.trim() && chainId.trim() && appId.trim());
  return (
    <div className="config">
      <label className="field field--wide">
        <span>Faucet URL</span>
        <input
          value={faucetUrl}
          placeholder="http://localhost:8079"
          onChange={(event) => onFaucetUrl(event.target.value)}
        />
      </label>
      <label className="field field--wide">
        <span>Chain ID</span>
        <input
          value={chainId}
          placeholder="chain id"
          onChange={(event) => onChainId(event.target.value)}
        />
      </label>
      <label className="field field--wide">
        <span>Application ID</span>
        <input
          value={appId}
          placeholder="application id"
          onChange={(event) => onAppId(event.target.value)}
        />
      </label>
      <button type="button" className="config__apply" disabled={!ready} onClick={onApply}>
        Apply
      </button>
    </div>
  );
}
