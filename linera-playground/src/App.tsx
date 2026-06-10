import { useEffect, useMemo, useRef, useState } from 'react';
import { GraphiQL } from 'graphiql';
import type { GraphiQLProps } from 'graphiql';
import 'graphiql/style.css';

import { LineraClientManager, type QueryTarget } from './linera/clientManager';
import { ephemeralSigner, metaMaskSigner, privateKeySigner } from './linera/signers';
import { makeFetcher } from './linera/fetcher';
import { ConfigBar } from './components/ConfigBar';
import { SignerControls } from './components/SignerControls';

const STORAGE_KEY = 'linera-playground.config';

interface PersistedConfig {
  faucetUrl: string;
  chainId: string;
  appId: string;
}

function loadConfig(): PersistedConfig {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      return JSON.parse(raw) as PersistedConfig;
    }
  } catch {
    // Ignore malformed storage.
  }
  return { faucetUrl: '', chainId: '', appId: '' };
}

export function App() {
  const initial = useMemo(loadConfig, []);
  const [faucetUrl, setFaucetUrl] = useState(initial.faucetUrl);
  const [chainId, setChainId] = useState(initial.chainId);
  const [appId, setAppId] = useState(initial.appId);
  const [owner, setOwner] = useState<string | null>(null);
  const [signerError, setSignerError] = useState<string | null>(null);
  // Bumping this remounts GraphiQL, which re-introspects the application schema.
  const [schemaToken, setSchemaToken] = useState(0);

  const managerRef = useRef<LineraClientManager | null>(null);
  if (!managerRef.current) {
    managerRef.current = new LineraClientManager(ephemeralSigner());
  }
  const manager = managerRef.current;

  // The fetcher reads the latest inputs at call time through this ref.
  const targetRef = useRef<Partial<QueryTarget>>({});
  targetRef.current = { faucetUrl, chainId, appId };

  const fetcher = useMemo(() => makeFetcher(manager, () => targetRef.current), [manager]);

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify({ faucetUrl, chainId, appId }));
    } catch {
      // Ignore storage failures (e.g. private mode).
    }
  }, [faucetUrl, chainId, appId]);

  async function useKey(secret: string) {
    try {
      const active = privateKeySigner(secret);
      await manager.setSigner(active);
      setOwner(active.owner);
      setSignerError(null);
    } catch (err) {
      setSignerError(err instanceof Error ? err.message : String(err));
    }
  }

  async function useMetaMask() {
    try {
      const active = await metaMaskSigner();
      await manager.setSigner(active);
      setOwner(active.owner);
      setSignerError(null);
    } catch (err) {
      setSignerError(err instanceof Error ? err.message : String(err));
    }
  }

  async function disconnect() {
    await manager.setSigner(ephemeralSigner());
    setOwner(null);
    setSignerError(null);
  }

  return (
    <div className="playground">
      <header className="playground__bar">
        <ConfigBar
          faucetUrl={faucetUrl}
          chainId={chainId}
          appId={appId}
          onFaucetUrl={setFaucetUrl}
          onChainId={setChainId}
          onAppId={setAppId}
          onApply={() => setSchemaToken((token) => token + 1)}
        />
        <SignerControls
          owner={owner}
          error={signerError}
          onUseKey={useKey}
          onUseMetaMask={useMetaMask}
          onDisconnect={disconnect}
        />
      </header>
      <main className="playground__editor">
        <GraphiQL key={schemaToken} fetcher={fetcher as unknown as GraphiQLProps['fetcher']} />
      </main>
    </div>
  );
}
