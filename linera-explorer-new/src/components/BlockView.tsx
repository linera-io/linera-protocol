import React, { useState, useEffect, useMemo } from 'react';
import { BlockList } from './BlockList';
import { SearchBar } from './SearchBar';
import { useBlocks } from '../hooks/useDatabase';

export const BlockView: React.FC = () => {
  const [currentPage, setCurrentPage] = useState(1);
  const [totalBlocks, setTotalBlocks] = useState(0);
  const [currentTime, setCurrentTime] = useState(new Date());

  const blocksPerPage = 50;
  const offset = (currentPage - 1) * blocksPerPage;

  const { blocks, latestBlock, loading, error } = useBlocks(blocksPerPage, offset);

  // Update current time every second for real-time "Last block seen" display
  useEffect(() => {
    const timer = setInterval(() => {
      setCurrentTime(new Date());
    }, 1000);

    return () => clearInterval(timer);
  }, []);

  // Fetch total block count
  useEffect(() => {
    const fetchCount = async () => {
      try {
        const api = new (await import('../utils/database')).BlockchainAPI();
        const count = await api.getTotalBlockCount();
        setTotalBlocks(count);
      } catch (err) {
        console.error('Failed to fetch block count:', err);
      }
    };
    fetchCount();
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

  const handleSearch = (query: string) => {
    // For now, just navigate to the block if it looks like a hash
    if (query.length >= 8) {
      window.location.href = `/block/${query}`;
    }
  };

  const totalPages = Math.ceil(totalBlocks / blocksPerPage);

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
      {totalPages > 1 && (
        <div className="flex justify-center items-center space-x-2">
          <button
            onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
            disabled={currentPage === 1}
            className="px-4 py-2 bg-linera-darker border border-linera-border rounded-lg text-white disabled:opacity-50 disabled:cursor-not-allowed hover:bg-linera-red hover:border-linera-red transition-colors"
          >
            Previous
          </button>

          {/* Page numbers */}
          <div className="flex space-x-2">
            {Array.from({ length: Math.min(totalPages, 7) }, (_, i) => {
              let pageNum;
              if (totalPages <= 7) {
                pageNum = i + 1;
              } else if (currentPage <= 4) {
                pageNum = i + 1;
              } else if (currentPage >= totalPages - 3) {
                pageNum = totalPages - 6 + i;
              } else {
                pageNum = currentPage - 3 + i;
              }

              return (
                <button
                  key={pageNum}
                  onClick={() => setCurrentPage(pageNum)}
                  className={`px-4 py-2 rounded-lg transition-colors ${
                    currentPage === pageNum
                      ? 'bg-linera-red text-white border border-linera-red'
                      : 'bg-linera-darker border border-linera-border text-white hover:bg-linera-red hover:border-linera-red'
                  }`}
                >
                  {pageNum}
                </button>
              );
            })}
          </div>

          <button
            onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
            disabled={currentPage === totalPages}
            className="px-4 py-2 bg-linera-darker border border-linera-border rounded-lg text-white disabled:opacity-50 disabled:cursor-not-allowed hover:bg-linera-red hover:border-linera-red transition-colors"
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
};
