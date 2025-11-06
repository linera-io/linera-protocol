import React from 'react';
import { useParams, Link } from 'react-router-dom';
import { ArrowLeft, Layers } from 'lucide-react';
import { useChainBlocks } from '../hooks/useDatabase';
import { BlockList } from './BlockList';
import { CopyableHash } from './CopyableHash';
import { useRelativeTime } from '../hooks/useRelativeTime';
import { Pagination } from './common/Pagination';
import { BlockchainAPI } from '../utils/database';
import { useBlocksPagination } from '../hooks/usePagination';

export const ChainDetail: React.FC = () => {
  const { chainId } = useParams<{ chainId: string }>();

  const { currentPage, setCurrentPage, totalPages, offset, itemsPerPage } = useBlocksPagination(
    async () => {
      if (!chainId) return 0;
      const api = new BlockchainAPI();
      return api.getChainBlockCount(chainId);
    }
  );

  const { blocks, latestBlock, loading, error } = useChainBlocks(chainId || '', itemsPerPage, offset);
  const latestBlockTime = useRelativeTime(latestBlock?.timestamp ?? null);

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
            <CopyableHash value={chainId || ''} format="full" />
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

      {/* Pagination */}
      <Pagination
        currentPage={currentPage}
        totalPages={totalPages}
        onPageChange={setCurrentPage}
      />
    </div>
  );
};