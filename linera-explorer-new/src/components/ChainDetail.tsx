import React, { useState, useEffect, useMemo } from 'react';
import { useParams, Link } from 'react-router-dom';
import { ArrowLeft, Layers, Copy } from 'lucide-react';
import { useChainBlocks } from '../hooks/useDatabase';
import { BlockList } from './BlockList';

export const ChainDetail: React.FC = () => {
  const { chainId } = useParams<{ chainId: string }>();
  const { blocks, latestBlock, loading, error } = useChainBlocks(chainId || '');
  const [currentTime, setCurrentTime] = useState(new Date());

  // Update current time every second for real-time "Last block seen" display
  useEffect(() => {
    const timer = setInterval(() => {
      setCurrentTime(new Date());
    }, 1000);
    
    return () => clearInterval(timer);
  }, []);
  
  // Calculate the latest block time - recalculates when currentTime or latestBlock changes
  const latestBlockTime = useMemo(() => {
    if (!latestBlock) return null;
    
    const blockTime = new Date(latestBlock.timestamp / 1000);
    const diffMs = currentTime.getTime() - blockTime.getTime();
    const diffSec = Math.floor(diffMs / 1000);
    
    if (diffSec < 60) return `${diffSec} seconds ago`;
    const diffMin = Math.floor(diffSec / 60);
    return `${diffMin} minutes ago`;
  }, [currentTime, latestBlock]);

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  return (
    <div className="space-y-8 animate-fade-in">
      {/* Header */}
      <div className="flex items-center justify-between">
        <Link
          to="/chains"
          className="flex items-center space-x-2 text-linera-gray-light hover:text-white transition-colors group"
        >
          <ArrowLeft className="w-5 h-5 group-hover:-translate-x-1 transition-transform" />
          <span>Back to Chains</span>
        </Link>
        
        <div className="flex items-center space-x-2">
          <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse"></div>
          <span className="text-sm text-linera-gray-light">Live Chain</span>
        </div>
      </div>

      {/* Chain Header */}
      <div className="relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-r from-blue-500/5 to-transparent"></div>
        <div className="relative">
          <div className="flex items-center space-x-4 mb-6">
            <div className="p-3 bg-blue-500/20 rounded-lg border border-blue-500/30">
              <Layers className="w-8 h-8 text-blue-400" />
            </div>
            <div>
              <h1 className="text-4xl font-epilogue font-bold text-gradient tracking-tight-custom">
                Chain Details
              </h1>
              <p className="text-lg text-linera-gray-light mt-2">
                {blocks.length} blocks shown
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Chain Info */}
      <div className="card">
        <div className="space-y-6">
          {/* Chain ID */}
          <div>
            <label className="block text-sm font-medium text-linera-gray-light mb-3">
              Chain ID
            </label>
            <div className="relative group">
              <div className="hash-display text-white pr-12">
                {chainId}
              </div>
              <button
                onClick={() => copyToClipboard(chainId || '')}
                className="absolute right-3 top-1/2 transform -translate-y-1/2 text-linera-gray-medium hover:text-linera-red"
              >
                <Copy className="w-4 h-4" />
              </button>
            </div>
          </div>

          {/* Stats */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="text-center p-4 bg-linera-darker/50 rounded-lg border border-linera-border/50">
              <div className="stat-number text-linera-red">{blocks.length}</div>
              <div className="text-sm text-linera-gray-light mt-1">Blocks Shown</div>
            </div>
            <div className="text-center p-4 bg-linera-darker/50 rounded-lg border border-linera-border/50">
              <div className="stat-number text-green-400">Active</div>
              <div className="text-sm text-linera-gray-light mt-1">Chain Status</div>
            </div>
            <div className="text-center p-4 bg-linera-darker/50 rounded-lg border border-linera-border/50">
              <div className="stat-number text-blue-400">
                {loading ? '...' : latestBlockTime || 'N/A'}
              </div>
              <div className="text-sm text-linera-gray-light mt-1">Last Block Seen</div>
            </div>
          </div>
        </div>
      </div>

      {/* Blocks List */}
      <div className="card">
        <div className="flex items-center space-x-3 mb-6">
          <div className="p-2 bg-linera-red/20 rounded-lg border border-linera-red/30">
            <Layers className="w-5 h-5 text-linera-red" />
          </div>
          <div>
            <h2 className="text-xl font-epilogue font-semibold text-white">
              Blocks in Chain
            </h2>
            <p className="text-sm text-linera-gray-light">
              Latest blocks for this chain
            </p>
          </div>
        </div>
        <BlockList blocks={blocks} loading={loading} error={error} />
      </div>
    </div>
  );
};