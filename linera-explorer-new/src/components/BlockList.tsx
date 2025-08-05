import React from 'react';
import { Link } from 'react-router-dom';
import { Clock, Hash, Layers, HardDrive, ChevronRight } from 'lucide-react';
import { BlockInfo } from '../types/blockchain';

interface BlockListProps {
  blocks: BlockInfo[];
  loading: boolean;
  error: string | null;
}

export const BlockList: React.FC<BlockListProps> = ({ blocks, loading, error }) => {
  const formatHash = (hash: string) => `${hash.slice(0, 8)}...${hash.slice(-8)}`;
  const formatChainId = (chainId: string) => `${chainId.slice(0, 12)}...`;
  const formatBytes = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffSec = Math.floor(diffMs / 1000);
    const diffMin = Math.floor(diffSec / 60);
    const diffHour = Math.floor(diffMin / 60);
    
    if (diffSec < 60) return `${diffSec}s ago`;
    if (diffMin < 60) return `${diffMin}m ago`;
    if (diffHour < 24) return `${diffHour}h ago`;
    return date.toLocaleDateString();
  };

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
          <Hash className="w-5 h-5 inline mr-2" />
          Error: {error}
        </div>
      </div>
    );
  }

  if (blocks.length === 0) {
    return (
      <div className="card text-center">
        <div className="text-linera-gray-light">
          <Layers className="w-12 h-12 mx-auto mb-4 opacity-50" />
          No blocks found
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {blocks.map((block, index) => (
        <Link
          key={block.hash}
          to={`/block/${block.hash}`}
          className="card group hover:scale-[1.01] transition-all duration-300 animate-slide-up"
          style={{ animationDelay: `${index * 50}ms` }}
        >
          <div className="flex items-center justify-between">
            <div className="flex-1 space-y-4">
              {/* Block Header */}
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  <div className="p-2 bg-linera-red/20 rounded-lg border border-linera-red/30">
                    <Hash className="w-4 h-4 text-linera-red" />
                  </div>
                  <div>
                    <div className="hash-display text-white">
                      {formatHash(block.hash)}
                    </div>
                    <div className="text-sm text-linera-gray-light mt-1">
                      Block Hash
                    </div>
                  </div>
                </div>
                
                <div className="text-right">
                  <div className="bg-linera-red/20 text-linera-red px-4 py-2 rounded-full text-sm font-semibold border border-linera-red/30">
                    #{block.height}
                  </div>
                </div>
              </div>
              
              {/* Block Details Grid */}
              <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
                <div className="flex items-center space-x-3">
                  <Layers className="w-4 h-4 text-linera-gray-medium" />
                  <div>
                    <div className="text-white font-medium">{block.height}</div>
                    <div className="text-sm text-linera-gray-light">Height</div>
                  </div>
                </div>
                
                <div className="flex items-center space-x-3">
                  <HardDrive className="w-4 h-4 text-linera-gray-medium" />
                  <div>
                    <div className="text-white font-medium">{formatBytes(block.size)}</div>
                    <div className="text-sm text-linera-gray-light">Size</div>
                  </div>
                </div>
                
                <div className="flex items-center space-x-3 col-span-2 md:col-span-1">
                  <Clock className="w-4 h-4 text-linera-gray-medium" />
                  <div>
                    <div className="text-white font-medium">{formatTimestamp(block.created_at)}</div>
                    <div className="text-sm text-linera-gray-light">Created</div>
                  </div>
                </div>
              </div>
              
              {/* Chain ID */}
              <div className="pt-2 border-t border-linera-border/50">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-2">
                    <span className="text-sm text-linera-gray-light">Chain:</span>
                    <span className="font-mono text-sm bg-linera-darker/80 px-3 py-1 rounded border border-linera-border/50 text-linera-gray-light">
                      {formatChainId(block.chain_id)}
                    </span>
                  </div>
                  
                  <ChevronRight className="w-5 h-5 text-linera-gray-medium group-hover:text-linera-red group-hover:translate-x-1 transition-all" />
                </div>
              </div>
            </div>
          </div>
        </Link>
      ))}
    </div>
  );
};