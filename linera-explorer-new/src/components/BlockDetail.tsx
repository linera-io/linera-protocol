import React from 'react';
import { useParams, Link } from 'react-router-dom';
import { ArrowLeft, Hash, Clock, Layers, HardDrive, Package, MessageSquare, Copy, ChevronRight } from 'lucide-react';
import { useBlock } from '../hooks/useDatabase';

export const BlockDetail: React.FC = () => {
  const { hash } = useParams<{ hash: string }>();
  const { block, bundles, loading, error } = useBlock(hash || '');

  const formatHash = (hash: string) => `${hash.slice(0, 12)}...${hash.slice(-12)}`;
  const formatFullHash = (hash: string) => hash;
  const formatBytes = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  if (loading) {
    return (
      <div className="flex justify-center items-center h-64">
        <div className="loading-spinner h-8 w-8"></div>
      </div>
    );
  }

  if (error || !block) {
    return (
      <div className="card bg-red-500/10 border-red-500/30">
        <div className="text-red-300">
          <Hash className="w-5 h-5 inline mr-2" />
          {error || 'Block not found'}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8 animate-fade-in">
      {/* Header */}
      <div className="flex items-center justify-between">
        <Link
          to="/"
          className="flex items-center space-x-2 text-linera-gray-light hover:text-white transition-colors group"
        >
          <ArrowLeft className="w-5 h-5 group-hover:-translate-x-1 transition-transform" />
          <span>Back to Blocks</span>
        </Link>
        
        <div className="flex items-center space-x-2">
          <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse"></div>
          <span className="text-sm text-linera-gray-light">Live Block</span>
        </div>
      </div>

      {/* Block Header */}
      <div className="relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-r from-linera-red/5 to-transparent"></div>
        <div className="relative">
          <div className="flex items-center space-x-4 mb-6">
            <div className="p-3 bg-linera-red/20 rounded-lg border border-linera-red/30">
              <Hash className="w-8 h-8 text-linera-red" />
            </div>
            <div>
              <h1 className="text-4xl font-epilogue font-bold text-gradient tracking-tight-custom">
                Block Details
              </h1>
              <p className="text-lg text-linera-gray-light mt-2">
                Block #{block.height} • {formatBytes(block.data.length)}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Block Info */}
      <div className="card">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <div className="space-y-6">
            {/* Hash */}
            <div>
              <label className="block text-sm font-medium text-linera-gray-light mb-3">
                Block Hash
              </label>
              <div className="relative group">
                <div className="hash-display text-white pr-12">
                  {formatFullHash(block.hash)}
                </div>
                <button
                  onClick={() => copyToClipboard(block.hash)}
                  className="absolute right-3 top-1/2 transform -translate-y-1/2 text-linera-gray-medium hover:text-linera-red"
                >
                  <Copy className="w-4 h-4" />
                </button>
              </div>
            </div>

            {/* Chain ID */}
            <div>
              <label className="block text-sm font-medium text-linera-gray-light mb-3">
                Chain ID
              </label>
              <div className="relative group">
                <div className="hash-display text-white pr-12">
                  {block.chain_id}
                </div>
                <button
                  onClick={() => copyToClipboard(block.chain_id)}
                  className="absolute right-3 top-1/2 transform -translate-y-1/2 text-linera-gray-medium hover:text-linera-red"
                >
                  <Copy className="w-4 h-4" />
                </button>
              </div>
            </div>
          </div>

          <div className="space-y-6">
            {/* Stats Grid */}
            <div className="grid grid-cols-1 gap-4">
              <div className="flex items-center justify-between p-4 bg-linera-darker/50 rounded-lg border border-linera-border/50">
                <div className="flex items-center space-x-3">
                  <Layers className="w-5 h-5 text-linera-gray-medium" />
                  <span className="text-linera-gray-light">Height</span>
                </div>
                <span className="stat-number text-white">{block.height}</span>
              </div>

              <div className="flex items-center justify-between p-4 bg-linera-darker/50 rounded-lg border border-linera-border/50">
                <div className="flex items-center space-x-3">
                  <HardDrive className="w-5 h-5 text-linera-gray-medium" />
                  <span className="text-linera-gray-light">Size</span>
                </div>
                <span className="stat-number text-white">{formatBytes(block.data.length)}</span>
              </div>

              <div className="flex items-center justify-between p-4 bg-linera-darker/50 rounded-lg border border-linera-border/50">
                <div className="flex items-center space-x-3">
                  <Clock className="w-5 h-5 text-linera-gray-medium" />
                  <span className="text-linera-gray-light">Timestamp</span>
                </div>
                <span className="text-white font-medium">{new Date(block.timestamp / 1000).toLocaleString()}</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Incoming Bundles */}
      {bundles.length > 0 && (
        <div className="card">
          <div className="flex items-center space-x-3 mb-6">
            <div className="p-2 bg-green-500/20 rounded-lg border border-green-500/30">
              <Package className="w-5 h-5 text-green-400" />
            </div>
            <div>
              <h2 className="text-xl font-epilogue font-semibold text-white">
                Incoming Bundles
              </h2>
              <p className="text-sm text-linera-gray-light">
                {bundles.length} bundle{bundles.length !== 1 ? 's' : ''} found
              </p>
            </div>
          </div>

          <div className="space-y-4">
            {bundles.map((bundle, index) => (
              <Link
                key={bundle.id}
                to={`/block/${bundle.source_cert_hash}`}
                className="block bg-linera-darker/30 border border-linera-border/50 rounded-lg p-6 animate-slide-up hover:bg-linera-darker/50 hover:border-linera-red/30 transition-all duration-300 group cursor-pointer"
                style={{ animationDelay: `${index * 100}ms` }}
              >
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  <div className="space-y-4">
                    <div className="flex items-center justify-between">
                      <span className="text-sm text-linera-gray-light">Bundle Index</span>
                      <span className="font-semibold text-white">{bundle.bundle_index}</span>
                    </div>
                    
                    <div className="space-y-2">
                      <span className="text-sm text-linera-gray-light">Origin Chain</span>
                      <div className="font-mono text-sm bg-linera-darker/80 px-3 py-2 rounded border border-linera-border/50 text-linera-gray-light group-hover:text-white transition-colors">
                        {formatHash(bundle.origin_chain_id)}
                      </div>
                    </div>
                    
                    <div className="flex items-center justify-between">
                      <span className="text-sm text-linera-gray-light">Action</span>
                      <span className={
                        bundle.action === 'Accept' 
                          ? 'status-badge-accept' 
                          : 'status-badge-reject'
                      }>
                        {bundle.action}
                      </span>
                    </div>
                  </div>

                  <div className="space-y-4">
                    <div className="flex items-center justify-between">
                      <span className="text-sm text-linera-gray-light">Source Height</span>
                      <span className="text-white font-medium">{bundle.source_height}</span>
                    </div>
                    
                    <div className="flex items-center justify-between">
                      <span className="text-sm text-linera-gray-light">Transaction Index</span>
                      <span className="text-white font-medium">{bundle.transaction_index}</span>
                    </div>
                    
                    <div className="space-y-2">
                      <span className="text-sm text-linera-gray-light">Timestamp</span>
                      <span className="text-white font-medium text-sm">
                        {new Date(bundle.source_timestamp / 1000).toLocaleString()}
                      </span>
                    </div>
                  </div>
                </div>

                {/* Source Block Info */}
                <div className="mt-4 pt-4 border-t border-linera-border/30 flex items-center justify-between">
                  <div className="flex items-center space-x-2">
                    <span className="text-sm text-linera-gray-light">Source Block:</span>
                    <span className="font-mono text-sm text-linera-gray-light group-hover:text-linera-red transition-colors">
                      {formatHash(bundle.source_cert_hash)}
                    </span>
                  </div>
                  
                  <div className="flex items-center space-x-2 text-linera-gray-medium group-hover:text-linera-red transition-colors">
                    <span className="text-sm">View Source Block</span>
                    <ChevronRight className="w-4 h-4 group-hover:translate-x-1 transition-transform" />
                  </div>
                </div>
              </Link>
            ))}
          </div>
        </div>
      )}

      {/* Raw Data Preview */}
      <div className="card">
        <div className="flex items-center space-x-3 mb-6">
          <div className="p-2 bg-purple-500/20 rounded-lg border border-purple-500/30">
            <MessageSquare className="w-5 h-5 text-purple-400" />
          </div>
          <div>
            <h2 className="text-xl font-epilogue font-semibold text-white">
              Raw Block Data
            </h2>
            <p className="text-sm text-linera-gray-light">
              Binary data • {formatBytes(block.data.length)}
            </p>
          </div>
        </div>
        
        <div className="bg-linera-darker/50 rounded-lg border border-linera-border/50 p-6 font-mono text-sm overflow-x-auto">
          <div className="text-linera-gray-light mb-4 flex items-center justify-between">
            <div>
              <span>Size: {formatBytes(block.data.length)}</span>
              <span className="mx-2">•</span>
              <span>Type: Binary Data</span>
            </div>
            <button
              onClick={() => copyToClipboard(Array.from(block.data).map(byte => byte.toString(16).padStart(2, '0')).join(''))}
              className="text-linera-gray-medium hover:text-linera-red transition-colors"
            >
              <Copy className="w-4 h-4" />
            </button>
          </div>
          <div className="text-linera-gray-light leading-relaxed">
            {Array.from(block.data.slice(0, 400))
              .map(byte => byte.toString(16).padStart(2, '0'))
              .join(' ')
              .replace(/(.{48})/g, '$1\n')}
            {block.data.length > 400 && (
              <div className="text-linera-gray-medium mt-4">
                ... and {block.data.length - 400} more bytes
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};