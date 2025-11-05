import React from 'react';
import { Link } from 'react-router-dom';
import { Clock, Hash, Layers, HardDrive, ChevronRight } from 'lucide-react';
import { BlockInfo } from '../types/blockchain';
import { formatHash, formatBytes, formatTimestamp, formatChainId } from '../utils/formatters';

interface BlockListProps {
  blocks: BlockInfo[];
  loading: boolean;
  error: string | null;
}

export const BlockList: React.FC<BlockListProps> = ({ blocks, loading, error }) => {

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
    <div className="space-y-4">
      {blocks.map((block, index) => (
        <Link
          key={block.hash}
          to={`/block/${block.hash}`}
          className="block bg-linera-card/90 backdrop-blur-sm border border-linera-border/80 rounded-xl p-6 transition-all duration-300 hover:bg-linera-card-hover hover:border-linera-red/50 hover:shadow-xl hover:shadow-linera-red/10 group animate-slide-up"
          style={{ animationDelay: `${index * 50}ms` }}
        >
          {/* Block Header */}
          <div className="flex items-center justify-between mb-6">
            <div className="flex items-center space-x-4">
              <div className="p-3 bg-linera-red/20 rounded-lg border border-linera-red/30 group-hover:bg-linera-red/30 transition-colors">
                <Hash className="w-5 h-5 text-linera-red" />
              </div>
              <div>
                <div className="font-mono text-lg font-semibold text-white group-hover:text-white transition-colors">
                  {formatHash(block.hash)}
                </div>
                <div className="text-sm text-linera-gray-light mt-1">
                  Block Hash
                </div>
              </div>
            </div>
            
            <div className="flex items-center space-x-3">
              <div className="bg-linera-red/20 text-linera-red px-4 py-2 rounded-full text-sm font-bold border border-linera-red/30 group-hover:bg-linera-red/30 transition-colors">
                #{block.height}
              </div>
              <ChevronRight className="w-5 h-5 text-linera-gray-medium group-hover:text-linera-red group-hover:translate-x-1 transition-all" />
            </div>
          </div>
          
          {/* Block Stats Grid */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6 mb-6">
            <div className="flex items-center space-x-3 p-3 bg-linera-darker/30 rounded-lg border border-linera-border/30">
              <Layers className="w-5 h-5 text-linera-gray-medium flex-shrink-0" />
              <div>
                <div className="text-white font-semibold">{block.height}</div>
                <div className="text-sm text-linera-gray-light">Height</div>
              </div>
            </div>
            
            <div className="flex items-center space-x-3 p-3 bg-linera-darker/30 rounded-lg border border-linera-border/30">
              <HardDrive className="w-5 h-5 text-linera-gray-medium flex-shrink-0" />
              <div>
                <div className="text-white font-semibold">{formatBytes(block.size)}</div>
                <div className="text-sm text-linera-gray-light">Size</div>
              </div>
            </div>
            
            <div className="flex items-center space-x-3 p-3 bg-linera-darker/30 rounded-lg border border-linera-border/30 sm:col-span-2 lg:col-span-1">
              <Clock className="w-5 h-5 text-linera-gray-medium flex-shrink-0" />
              <div>
                <div className="text-white font-semibold">{formatTimestamp(new Date(block.timestamp / 1000).toISOString())}</div>
                <div className="text-sm text-linera-gray-light">Created</div>
              </div>
            </div>
          </div>
          
          {/* Chain ID Section */}
          <div className="pt-4 border-t border-linera-border/50">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <span className="text-sm font-medium text-linera-gray-light">Chain ID:</span>
                <span className="font-mono text-sm bg-linera-darker/50 px-3 py-2 rounded-lg border border-linera-border/50 text-linera-gray-light group-hover:text-white transition-colors">
                  {formatChainId(block.chain_id)}
                </span>
              </div>
              
              <div className="text-sm text-linera-gray-medium group-hover:text-linera-red transition-colors flex items-center space-x-1">
                <span>View Details</span>
              </div>
            </div>
          </div>
        </Link>
      ))}
    </div>
  );
};