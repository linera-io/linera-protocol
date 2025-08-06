import { useState, useEffect, useRef } from 'react';
import { BlockchainAPI } from '../utils/database';
import { BlockInfo, Block, IncomingBundle, ChainInfo } from '../types/blockchain';

const api = new BlockchainAPI();

export const useAPI = () => {
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Test API connection
    const testConnection = async () => {
      try {
        const response = await fetch('http://localhost:3002/api/health');
        if (response.ok) {
          setIsConnected(true);
        } else {
          throw new Error('API server not responding');
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to connect to API');
      }
    };

    testConnection();
  }, []);

  return { isConnected, error, api };
};

// Generic polling hook to reduce code duplication
const usePollingData = <T>(
  fetcher: () => Promise<T>,
  dependencies: any[],
  options: {
    refreshInterval?: number;
    logPrefix?: string;
    errorMessage?: string;
    enabled?: boolean;
  } = {}
) => {
  const {
    refreshInterval = 5000,
    logPrefix = '🔄',
    errorMessage = 'Failed to fetch data',
    enabled = true
  } = options;

  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!enabled) return;

    const fetchData = async (isPolling = false) => {
      try {
        if (!isPolling) {
          setLoading(true);
        }
        
        const result = await fetcher();
        
        if (isPolling) {
          console.log(`${logPrefix} Refreshed data from API`);
        }
        
        setData(result);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : errorMessage);
      } finally {
        if (!isPolling) {
          setLoading(false);
        }
      }
    };

    // Initial fetch
    fetchData();

    // Set up polling
    const pollInterval = setInterval(() => {
      console.log(`${logPrefix} Polling for updates...`);
      fetchData(true);
    }, refreshInterval);

    // Cleanup interval on unmount or dependency change
    return () => {
      clearInterval(pollInterval);
    };
  }, [enabled, refreshInterval, ...dependencies]);

  return { data, loading, error };
};

export const useBlocks = (limit: number = 50, refreshInterval: number = 5000) => {
  const [latestBlock, setLatestBlock] = useState<BlockInfo | null>(null);
  const { isConnected } = useAPI();
  const latestBlockRef = useRef<BlockInfo | null>(null);

  const { data: blocks, loading, error } = usePollingData<BlockInfo[]>(
    () => api.getBlocks(limit, 0),
    [limit],
    {
      refreshInterval,
      logPrefix: '📋',
      errorMessage: 'Failed to fetch blocks',
      enabled: isConnected
    }
  );

  // Handle latestBlock tracking when blocks data changes
  useEffect(() => {
    if (blocks && blocks.length > 0) {
      const newLatestBlock = blocks[0];
      // Only update if we don't have a latest block yet, or if the new one is actually newer
      if (!latestBlockRef.current || new Date(newLatestBlock.timestamp / 1000) > new Date(latestBlockRef.current.timestamp / 1000)) {
        setLatestBlock(newLatestBlock);
        latestBlockRef.current = newLatestBlock;
      }
    }
  }, [blocks]);

  return { blocks: blocks || [], latestBlock, loading, error };
};

export const useBlock = (hash: string) => {
  const [block, setBlock] = useState<Block | null>(null);
  const [bundles, setBundles] = useState<IncomingBundle[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { isConnected } = useAPI();

  useEffect(() => {
    if (!isConnected || !hash) return;

    const fetchBlock = async () => {
      try {
        setLoading(true);
        const [blockResult, bundlesResult] = await Promise.all([
          api.getBlockByHash(hash),
          api.getIncomingBundles(hash)
        ]);
        
        setBlock(blockResult);
        setBundles(bundlesResult);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch block');
      } finally {
        setLoading(false);
      }
    };

    fetchBlock();
  }, [isConnected, hash]);

  return { block, bundles, loading, error };
};

export const useChains = (refreshInterval: number = 5000) => {
  const { isConnected } = useAPI();

  const { data: chains, loading, error } = usePollingData<ChainInfo[]>(
    () => api.getChains(),
    [],
    {
      refreshInterval,
      logPrefix: '⛓️',
      errorMessage: 'Failed to fetch chains',
      enabled: isConnected
    }
  );

  return { chains: chains || [], loading, error };
};

export const useChainBlocks = (chainId: string, limit: number = 50, refreshInterval: number = 5000) => {
  const [latestBlock, setLatestBlock] = useState<BlockInfo | null>(null);
  const { isConnected } = useAPI();
  const latestBlockRef = useRef<BlockInfo | null>(null);

  const { data: blocks, loading, error } = usePollingData<BlockInfo[]>(
    () => api.getBlocksByChain(chainId, limit, 0),
    [chainId, limit],
    {
      refreshInterval,
      logPrefix: '🔗',
      errorMessage: 'Failed to fetch chain blocks',
      enabled: isConnected && !!chainId
    }
  );

  // Handle latestBlock tracking when blocks data changes
  useEffect(() => {
    if (blocks && blocks.length > 0) {
      const newLatestBlock = blocks[0];
      // Only update if we don't have a latest block yet, or if the new one is actually newer
      if (!latestBlockRef.current || new Date(newLatestBlock.timestamp / 1000) > new Date(latestBlockRef.current.timestamp / 1000)) {
        setLatestBlock(newLatestBlock);
        latestBlockRef.current = newLatestBlock;
      }
    }
  }, [blocks]);

  return { blocks: blocks || [], latestBlock, loading, error };
};