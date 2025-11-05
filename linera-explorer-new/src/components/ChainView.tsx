import React from 'react';
import { Link } from 'react-router-dom';
import { Layers, TrendingUp, Hash, ChevronRight } from 'lucide-react';
import { useChains } from '../hooks/useDatabase';
import { ChainInfo } from '../types/blockchain';
import { formatHash, formatChainId } from '../utils/formatters';
import { Pagination } from './common/Pagination';
import { LoadingSpinner } from './common/LoadingSpinner';
import { ErrorMessage } from './common/ErrorMessage';
import { BlockchainAPI } from '../utils/database';

export const ChainView: React.FC = () => {
  const [currentPage, setCurrentPage] = React.useState(1);
  const [searchQuery, setSearchQuery] = React.useState('');
  const [searchResult, setSearchResult] = React.useState<ChainInfo[] | null>(null);
  const [searchLoading, setSearchLoading] = React.useState(false);
  const [searchError, setSearchError] = React.useState<string | null>(null);
  const [totalChains, setTotalChains] = React.useState(0);

  const chainsPerPage = 50;
  const offset = (currentPage - 1) * chainsPerPage;

  const { chains, loading, error } = useChains(chainsPerPage, offset);

  // Fetch total chain count
  React.useEffect(() => {
    const fetchCount = async () => {
      try {
        const api = new BlockchainAPI();
        const count = await api.getChainsCount();
        setTotalChains(count);
      } catch (err) {
        console.error('Failed to fetch chain count:', err);
      }
    };
    fetchCount();
  }, []);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!searchQuery.trim()) {
      setSearchResult(null);
      setSearchError(null);
      return;
    }

    // Validate hex string format
    if (!/^[0-9a-f]{64}$/i.test(searchQuery)) {
      setSearchError('Chain ID must be a 64-character hex string');
      setSearchResult(null);
      return;
    }

    setSearchLoading(true);
    setSearchError(null);

    try {
      const api = new BlockchainAPI();
      const result = await api.getChainById(searchQuery);
      if (result) {
        setSearchResult([result]);
      } else {
        setSearchResult([]);
        setSearchError('No chain found with this ID');
      }
    } catch (err) {
      setSearchError(err instanceof Error ? err.message : 'Search failed');
      setSearchResult(null);
    } finally {
      setSearchLoading(false);
    }
  };

  const clearSearch = () => {
    setSearchQuery('');
    setSearchResult(null);
    setSearchError(null);
  };

  const totalPages = Math.ceil(totalChains / chainsPerPage);
  const displayChains = searchResult || chains;
  const isSearchMode = searchResult !== null || searchQuery.trim() !== '';

  if (loading && !isSearchMode) {
    return <LoadingSpinner />;
  }

  if (error && !isSearchMode) {
    return <ErrorMessage message={error} icon={<Layers className="w-5 h-5" />} />;
  }

  return (
    <div className="space-y-8 animate-fade-in">
      {/* Header */}
      <div className="relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-r from-blue-500/5 to-transparent"></div>
        <div className="relative">
          <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-6">
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

            {/* Search Box */}
            <div className="lg:w-96">
              <form onSubmit={handleSearch} className="relative">
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  placeholder="Search by Chain ID (64-char hex)"
                  className="w-full px-4 py-3 bg-linera-darker border border-linera-border rounded-lg text-white placeholder-linera-gray-medium focus:outline-none focus:ring-2 focus:ring-linera-red focus:border-transparent"
                />
                {searchQuery && (
                  <button
                    type="button"
                    onClick={clearSearch}
                    className="absolute right-3 top-1/2 transform -translate-y-1/2 text-linera-gray-medium hover:text-white"
                  >
                    ✕
                  </button>
                )}
              </form>
              {searchError && (
                <div className="mt-2 text-sm text-red-400">{searchError}</div>
              )}
            </div>
          </div>
        </div>
      </div>

      {searchLoading ? (
        <LoadingSpinner />
      ) : displayChains.length === 0 ? (
        <div className="card text-center">
          <div className="text-linera-gray-light">
            <Layers className="w-12 h-12 mx-auto mb-4 opacity-50" />
            {isSearchMode ? 'No chains found' : 'No chains available'}
          </div>
        </div>
      ) : (
        <>
          {/* Chains Grid */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {displayChains.map((chain, index) => (
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

          {/* Pagination - only show when not in search mode */}
          {!isSearchMode && (
            <Pagination
              currentPage={currentPage}
              totalPages={totalPages}
              onPageChange={setCurrentPage}
            />
          )}
        </>
      )}
    </div>
  );
};