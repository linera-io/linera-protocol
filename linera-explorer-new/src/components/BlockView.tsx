import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { BlockList } from './BlockList';
import { SearchBar } from './SearchBar';
import { Pagination } from './common/Pagination';
import { useBlocks } from '../hooks/useDatabase';
import { useRelativeTime } from '../hooks/useRelativeTime';
import { BlockchainAPI } from '../utils/database';
import { useBlocksPagination } from '../hooks/usePagination';
import { useLocalSearch } from '../hooks/useLocalSearch';
import { BlockInfo } from '../types/blockchain';

export const BlockView: React.FC = () => {
  const navigate = useNavigate();
  const [searchError, setSearchError] = useState<string | null>(null);
  const [searchLoading, setSearchLoading] = useState(false);

  const { currentPage, setCurrentPage, totalPages, offset, itemsPerPage } = useBlocksPagination(
    async () => {
      const api = new BlockchainAPI();
      return api.getTotalBlockCount();
    }
  );

  const { blocks, latestBlock, loading, error } = useBlocks(itemsPerPage, offset);
  const latestBlockTime = useRelativeTime(latestBlock?.timestamp ?? null);

  const { search } = useLocalSearch<BlockInfo>();

  const handleSearch = async (query: string) => {
    setSearchError(null);

    // Search locally first with validation
    const result = search(query, {
      items: blocks,
      getSearchValue: (block) => block.hash
    });

    if (result.error) {
      setSearchError(result.error);
      return;
    }

    if (result.found && result.item) {
      // Block found in current page, navigate directly
      navigate(`/block/${result.item.hash}`);
      return;
    }

    // Not in current page, validate with server before navigating
    setSearchLoading(true);
    setSearchError(null);

    try {
      const api = new BlockchainAPI();
      const serverResult = await api.getBlockByHash(query);
      if (serverResult) {
        // Block exists, navigate to it
        navigate(`/block/${query}`);
      } else {
        setSearchError('No block found with this hash');
      }
    } catch (err) {
      setSearchError(err instanceof Error ? err.message : 'Search failed');
    } finally {
      setSearchLoading(false);
    }
  };

  return (
    <div className="space-y-8 animate-fade-in">
      {/* Hero Section */}
      <div className="relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-r from-linera-red/5 to-transparent"></div>
        <div className="relative">
          <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-6">
            <div className="space-y-4">
              <h1 className="text-4xl lg:text-5xl font-epilogue font-bold text-gradient tracking-tight-custom">
                Latest Blocks
              </h1>
              <p className="text-lg text-linera-gray-light max-w-2xl">
                Explore the most recent blocks in the Linera blockchain network.
                Real-time insights into transactions and chain activity.
              </p>
            </div>

            <div className="lg:w-96">
              <SearchBar onSearch={handleSearch} />
              {searchError && (
                <div className="mt-2 text-sm text-red-400">{searchError}</div>
              )}
              {searchLoading && (
                <div className="mt-2 text-sm text-linera-gray-light">Searching...</div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Stats Overview */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="card stat-card">
          <div className="stat-number text-linera-red">{blocks.length}</div>
          <div className="text-linera-gray-light mt-1">Blocks Shown</div>
        </div>
        <div className="card stat-card">
          <div className="stat-number text-green-400">Live</div>
          <div className="text-linera-gray-light mt-1">Network Status</div>
        </div>
        <div className="card stat-card">
          <div className="stat-number text-blue-400">
            {loading ? '...' : latestBlockTime || 'N/A'}
          </div>
          <div className="text-linera-gray-light mt-1">Last Block Seen</div>
        </div>
      </div>

      {/* Blocks List */}
      <div className="space-y-4">
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
