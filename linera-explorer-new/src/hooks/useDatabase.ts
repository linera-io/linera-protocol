import { useState, useEffect, useRef, useCallback } from 'react';
import { BlockchainAPI, API_BASE_URL } from '../utils/database';
import { BlockInfo, Block, IncomingBundleWithMessages, ChainInfo, Operation, Message, Event, OracleResponse } from '../types/blockchain';

const api = new BlockchainAPI();

export const useAPI = () => {
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Test API connection using the configured API base URL
    const testConnection = async () => {
      try {
        const response = await fetch(`${API_BASE_URL}/health`);
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
  const [bundles, setBundles] = useState<IncomingBundleWithMessages[]>([]);
  const [operations, setOperations] = useState<Operation[]>([]);
  const [messages, setMessages] = useState<Message[]>([]);
  const [events, setEvents] = useState<Event[]>([]);
  const [oracleResponses, setOracleResponses] = useState<OracleResponse[]>([]);
  const [activity, setActivity] = useState<{
    operationsCount: number;
    messagesCount: number;
    eventsCount: number;
    oracleResponsesCount: number;
  } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { isConnected } = useAPI();

  useEffect(() => {
    if (!isConnected || !hash) return;

    const fetchBlock = async () => {
      try {
        setLoading(true);
        const [
          blockResult,
          bundlesResult,
          operationsResult,
          messagesResult,
          eventsResult,
          oracleResponsesResult
        ] = await Promise.all([
          api.getBlockByHash(hash),
          api.getBundlesWithMessages(hash), // Use optimized query
          api.getOperations(hash),
          api.getMessages(hash),
          api.getEvents(hash),
          api.getOracleResponses(hash)
        ]);
        
        // Calculate activity counts from the data we already have
        const activityResult = {
          operationsCount: operationsResult.length,
          messagesCount: messagesResult.length,
          eventsCount: eventsResult.length,
          oracleResponsesCount: oracleResponsesResult.length
        };
        
        setBlock(blockResult);
        setBundles(bundlesResult);
        setOperations(operationsResult);
        setMessages(messagesResult);
        setEvents(eventsResult);
        setOracleResponses(oracleResponsesResult);
        setActivity(activityResult);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch block');
      } finally {
        setLoading(false);
      }
    };

    fetchBlock();
  }, [isConnected, hash]);

  return { 
    block, 
    bundles, 
    operations, 
    messages, 
    events, 
    oracleResponses, 
    activity, 
    loading, 
    error 
  };
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