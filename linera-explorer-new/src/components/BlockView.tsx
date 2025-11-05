import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { BlockList } from './BlockList';
import { SearchBar } from './SearchBar';
import { Pagination } from './common/Pagination';
import { useBlocks } from '../hooks/useDatabase';
import { useRelativeTime } from '../hooks/useRelativeTime';
import { BlockchainAPI } from '../utils/database';
import { BLOCKS_PER_PAGE } from '../config/constants';

export const BlockView: React.FC = () => {
  const navigate = useNavigate();
  const [currentPage, setCurrentPage] = useState(1);
  const [totalBlocks, setTotalBlocks] = useState(0);

  const offset = (currentPage - 1) * BLOCKS_PER_PAGE;

  const { blocks, latestBlock, loading, error } = useBlocks(BLOCKS_PER_PAGE, offset);
  const latestBlockTime = useRelativeTime(latestBlock?.timestamp ?? null);

  // Fetch total block count
  useEffect(() => {
    const fetchCount = async () => {
      try {
        const api = new BlockchainAPI();
        const count = await api.getTotalBlockCount();
        setTotalBlocks(count);
      } catch (err) {
        console.error('Failed to fetch block count:', err);
      }
    };
    fetchCount();
  }, []);

  const handleSearch = (query: string) => {
    // Navigate to the block if it looks like a hash
    if (query.length >= 8) {
      navigate(`/block/${query}`);
    }
  };

  const totalPages = Math.ceil(totalBlocks / BLOCKS_PER_PAGE);

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
      <Pagination
        currentPage={currentPage}
        totalPages={totalPages}
        onPageChange={setCurrentPage}
      />
    </div>
  );
};
