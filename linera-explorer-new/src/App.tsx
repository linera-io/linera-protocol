import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link, useLocation } from 'react-router-dom';
import { Layers, Home, Activity } from 'lucide-react';
import { BlockView } from './components/BlockView';
import { BlockDetail } from './components/BlockDetail';
import { ChainView } from './components/ChainView';
import { ChainDetail } from './components/ChainDetail';

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

const App: React.FC = () => {
  return (
    <Router>
      <div className="min-h-screen bg-linera-dark">
        <Navigation />
        
        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
          <Routes>
            <Route path="/" element={<BlockView />} />
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