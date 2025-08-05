import { useState, useEffect } from 'react';
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

export const useBlocks = (limit: number = 50, refreshInterval: number = 5000) => {
  const [blocks, setBlocks] = useState<BlockInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { isConnected } = useAPI();

  useEffect(() => {
    if (!isConnected) return;

    const fetchBlocks = async (isPolling = false) => {
      try {
        if (!isPolling) {
          setLoading(true);
        }
        // Always fetch from beginning (offset=0) to get the latest blocks
        const result = await api.getBlocks(limit, 0);
        
        if (isPolling) {
          console.log('📋 Refreshed blocks from API');
        }
        
        setBlocks(result);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch blocks');
      } finally {
        if (!isPolling) {
          setLoading(false);
        }
      }
    };

    // Initial fetch
    fetchBlocks();

    // Set up polling
    const pollInterval = setInterval(() => {
      console.log('📋 Polling for new blocks...');
      fetchBlocks(true);
    }, refreshInterval);

    // Cleanup interval on unmount or dependency change
    return () => clearInterval(pollInterval);
  }, [isConnected, limit, refreshInterval]);

  return { blocks, loading, error };
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

export const useChains = () => {
  const [chains, setChains] = useState<ChainInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { isConnected } = useAPI();

  useEffect(() => {
    if (!isConnected) return;

    const fetchChains = async () => {
      try {
        setLoading(true);
        const result = await api.getChains();
        setChains(result);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch chains');
      } finally {
        setLoading(false);
      }
    };

    fetchChains();
  }, [isConnected]);

  return { chains, loading, error };
};

export const useChainBlocks = (chainId: string, limit: number = 50, refreshInterval: number = 5000) => {
  const [blocks, setBlocks] = useState<BlockInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { isConnected } = useAPI();

  useEffect(() => {
    if (!isConnected || !chainId) return;

    const fetchChainBlocks = async (isPolling = false) => {
      try {
        if (!isPolling) {
          setLoading(true);
        }
        // Always fetch from beginning (offset=0) to get the latest blocks for this chain
        const result = await api.getBlocksByChain(chainId, limit, 0);
        
        if (isPolling) {
          console.log(`🔗 Refreshed blocks for chain ${chainId}`);
        }
        
        setBlocks(result);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch chain blocks');
      } finally {
        if (!isPolling) {
          setLoading(false);
        }
      }
    };

    // Initial fetch
    fetchChainBlocks();

    // Set up polling
    const pollInterval = setInterval(() => {
      console.log(`🔗 Polling for new blocks on chain ${chainId}...`);
      fetchChainBlocks(true);
    }, refreshInterval);

    // Cleanup interval on unmount or dependency change
    return () => clearInterval(pollInterval);
  }, [isConnected, chainId, limit, refreshInterval]);

  return { blocks, loading, error };
};