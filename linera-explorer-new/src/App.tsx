import React, { useState, useEffect, useMemo } from 'react';
import { BrowserRouter as Router, Routes, Route, Link, useLocation } from 'react-router-dom';
import { Layers, Home, Activity } from 'lucide-react';
import { BlockList } from './components/BlockList';
import { BlockDetail } from './components/BlockDetail';
import { ChainView } from './components/ChainView';
import { ChainDetail } from './components/ChainDetail';
import { SearchBar } from './components/SearchBar';
import { useBlocks } from './hooks/useDatabase';

const Navigation: React.FC = () => {
  const location = useLocation();
  
  const isActive = (path: string) => {
    if (path === '/' && location.pathname === '/') return true;
    if (path !== '/' && location.pathname.startsWith(path)) return true;
    return false;
  };

  return (
    <nav className="border-b border-linera-border bg-linera-card/30 backdrop-blur-lg sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between h-20">
          <div className="flex items-center space-x-8">
            <Link to="/" className="flex items-center space-x-3 group">
              <div className="p-2 bg-linera-red/20 rounded-lg border border-linera-red/30 group-hover:bg-linera-red/30 transition-colors">
                <Activity className="w-6 h-6 text-linera-red" />
              </div>
              <div className="flex flex-col">
                <span className="text-xl font-epilogue font-bold text-white tracking-tight-custom">
                  Linera
                </span>
                <span className="text-sm text-linera-gray-light font-medium">
                  Explorer
                </span>
              </div>
            </Link>
            
            <div className="flex space-x-2 ml-8">
              <Link
                to="/"
                className={`nav-link ${
                  isActive('/') && location.pathname === '/' ? 'active' : ''
                }`}
              >
                <Home className="w-4 h-4" />
                <span>Blocks</span>
              </Link>
              
              <Link
                to="/chains"
                className={`nav-link ${
                  isActive('/chains') ? 'active' : ''
                }`}
              >
                <Layers className="w-4 h-4" />
                <span>Chains</span>
              </Link>
            </div>
          </div>
          
          <div className="flex items-center">
            <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse mr-2"></div>
            <span className="text-sm text-linera-gray-light">Live</span>
          </div>
        </div>
      </div>
    </nav>
  );
};

const HomePage: React.FC = () => {
  const { blocks, latestBlock, loading, error } = useBlocks();
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

  const handleSearch = (query: string) => {
    // For now, just navigate to the block if it looks like a hash
    if (query.length >= 8) {
      window.location.href = `/block/${query}`;
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
    </div>
  );
};

const App: React.FC = () => {
  return (
    <Router>
      <div className="min-h-screen bg-linera-dark">
        <Navigation />
        
        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
          <Routes>
            <Route path="/" element={<HomePage />} />
            <Route path="/block/:hash" element={<BlockDetail />} />
            <Route path="/chains" element={<ChainView />} />
            <Route path="/chain/:chainId" element={<ChainDetail />} />
          </Routes>
        </main>
        
        {/* Background decorative elements */}
        <div className="fixed inset-0 -z-10 overflow-hidden pointer-events-none">
          <div className="absolute top-1/4 left-1/4 w-96 h-96 bg-linera-red/5 rounded-full blur-3xl"></div>
          <div className="absolute bottom-1/4 right-1/4 w-96 h-96 bg-blue-500/5 rounded-full blur-3xl"></div>
        </div>
      </div>
    </Router>
  );
};

export default App;