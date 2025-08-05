import { useState, useEffect } from 'react';
import { BlockchainAPI } from '../utils/database';
import { BlockInfo, Block, IncomingBundle, PostedMessage, ChainInfo } from '../types/blockchain';

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

export const useBlocks = (limit: number = 50, offset: number = 0) => {
  const [blocks, setBlocks] = useState<BlockInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { isConnected } = useAPI();

  useEffect(() => {
    if (!isConnected) return;

    const fetchBlocks = async () => {
      try {
        setLoading(true);
        const result = await api.getBlocks(limit, offset);
        setBlocks(result);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch blocks');
      } finally {
        setLoading(false);
      }
    };

    fetchBlocks();
  }, [isConnected, limit, offset]);

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

export const useChainBlocks = (chainId: string, limit: number = 50, offset: number = 0) => {
  const [blocks, setBlocks] = useState<BlockInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { isConnected } = useAPI();

  useEffect(() => {
    if (!isConnected || !chainId) return;

    const fetchChainBlocks = async () => {
      try {
        setLoading(true);
        const result = await api.getBlocksByChain(chainId, limit, offset);
        setBlocks(result);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch chain blocks');
      } finally {
        setLoading(false);
      }
    };

    fetchChainBlocks();
  }, [isConnected, chainId, limit, offset]);

  return { blocks, loading, error };
};