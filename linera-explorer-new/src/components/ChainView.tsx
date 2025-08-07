import React from 'react';
import { Link } from 'react-router-dom';
import { Layers, TrendingUp, Hash, ChevronRight } from 'lucide-react';
import { useChains } from '../hooks/useDatabase';

export const ChainView: React.FC = () => {
  const { chains, loading, error } = useChains();

  const formatChainId = (chainId: string) => `${chainId.slice(0, 16)}...${chainId.slice(-8)}`;
  const formatHash = (hash: string) => `${hash.slice(0, 8)}...${hash.slice(-8)}`;

  if (loading) {
    return (
      <div className="flex justify-center items-center h-64">
        <div className="loading-spinner h-8 w-8"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="card bg-red-500/10 border-red-500/30">
        <div className="text-red-300">
          <Layers className="w-5 h-5 inline mr-2" />
          Error: {error}
        </div>
      </div>
    );
  }

  if (chains.length === 0) {
    return (
      <div className="card text-center">
        <div className="text-linera-gray-light">
          <Layers className="w-12 h-12 mx-auto mb-4 opacity-50" />
          No chains found
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8 animate-fade-in">
      {/* Header */}
      <div className="relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-r from-blue-500/5 to-transparent"></div>
        <div className="relative">
          <div className="flex items-center space-x-4">
            <div className="p-3 bg-blue-500/20 rounded-lg border border-blue-500/30">
              <Layers className="w-8 h-8 text-blue-400" />
            </div>
            <div>
              <h1 className="text-4xl font-epilogue font-bold text-gradient tracking-tight-custom">
                Blockchain Chains
              </h1>
              <p className="text-lg text-linera-gray-light mt-2">
                Explore all active chains in the Linera network
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Chains Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {chains.map((chain, index) => (
          <Link
            key={chain.chain_id}
            to={`/chain/${chain.chain_id}`}
            className="card group hover:scale-[1.02] transition-all duration-300 animate-slide-up"
            style={{ animationDelay: `${index * 100}ms` }}
          >
            <div className="space-y-6">
              {/* Chain Header */}
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  <div className="p-2 bg-linera-red/20 rounded-lg border border-linera-red/30">
                    <Hash className="w-5 h-5 text-linera-red" />
                  </div>
                  <div>
                    <div className="text-white font-semibold">Chain</div>
                    <div className="text-sm text-linera-gray-light">Network</div>
                  </div>
                </div>
                <ChevronRight className="w-5 h-5 text-linera-gray-medium group-hover:text-linera-red group-hover:translate-x-1 transition-all" />
              </div>

              {/* Chain ID */}
              <div>
                <div className="text-sm text-linera-gray-light mb-2">Chain ID</div>
                <div className="hash-display text-white">
                  {formatChainId(chain.chain_id)}
                </div>
              </div>

              {/* Stats Grid */}
              <div className="grid grid-cols-2 gap-4">
                <div className="text-center p-4 bg-linera-darker/50 rounded-lg border border-linera-border/50">
                  <div className="stat-number text-linera-red">
                    {chain.block_count}
                  </div>
                  <div className="text-sm text-linera-gray-light mt-1">Blocks</div>
                </div>
                
                <div className="text-center p-4 bg-linera-darker/50 rounded-lg border border-linera-border/50">
                  <div className="stat-number text-green-400">
                    {chain.latest_height}
                  </div>
                  <div className="text-sm text-linera-gray-light mt-1">Height</div>
                </div>
              </div>

              {/* Latest Block */}
              <div className="pt-4 border-t border-linera-border/50">
                <div className="flex items-center space-x-2 mb-3">
                  <TrendingUp className="w-4 h-4 text-linera-gray-medium" />
                  <span className="text-sm text-linera-gray-light">Latest Block</span>
                </div>
                <div className="font-mono text-sm bg-linera-darker/80 px-3 py-2 rounded border border-linera-border/50 text-linera-gray-light">
                  {formatHash(chain.latest_block_hash)}
                </div>
              </div>

              {/* Action Button */}
              <div className="pt-2">
                <div className="btn-secondary w-full justify-center group-hover:bg-linera-red group-hover:text-white group-hover:border-linera-red">
                  <span>Explore Chain</span>
                  <ChevronRight className="w-4 h-4 ml-2" />
                </div>
              </div>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
};