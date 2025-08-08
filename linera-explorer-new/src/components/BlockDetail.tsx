import React from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import { ArrowLeft, Hash, Clock, Layers, HardDrive, Package, MessageSquare, ChevronRight, Settings, Mail, Zap, Database, MessageCircle } from 'lucide-react';
import { useBlock } from '../hooks/useDatabase';
import { ExpandableSection } from './ExpandableSection';
import { CopyableHash } from './CopyableHash';
import { BinaryDataSection } from './BinaryDataSection';

export const BlockDetail: React.FC = () => {
  const { hash } = useParams<{ hash: string }>();
  const navigate = useNavigate();
  const { block, bundles, operations, messages, events, oracleResponses, activity, loading, error } = useBlock(hash || '');

  const formatBytes = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
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
                Block #{block.height} • {activity ? `${activity.operationsCount + activity.messagesCount + activity.eventsCount + activity.oracleResponsesCount} activities` : formatBytes(block.data.length)}
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
                <CopyableHash value={block.hash} format="full" />
              </div>
            </div>

            {/* Chain ID */}
            <div>
              <label className="block text-sm font-medium text-linera-gray-light mb-3">
                Chain ID
              </label>
              <CopyableHash value={block.chain_id} format="full" />
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

      {/* Block Activities */}
      {activity && (
        <>
          {/* Operations */}
          <ExpandableSection
            title="Operations"
            count={activity.operationsCount}
            icon={<Settings className="w-5 h-5 text-linera-red" />}
          >
            <div className="space-y-3">
              {operations.map((operation) => (
                <div key={operation.id} className="bg-linera-darker/30 border border-linera-border/50 rounded-lg p-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-linera-gray-light">Index</span>
                        <span className="text-white font-medium">{operation.operation_index}</span>
                      </div>
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-linera-gray-light">Type</span>
                        <span className={`px-2 py-1 rounded text-xs font-medium ${
                          operation.operation_type === 'System' 
                            ? 'bg-blue-500/20 text-blue-400 border border-blue-500/30' 
                            : 'bg-purple-500/20 text-purple-400 border border-purple-500/30'
                        }`}>
                          {operation.operation_type}
                        </span>
                      </div>
                    </div>
                    <div className="space-y-2">
                      {operation.system_operation_type && (
                        <div className="flex items-center justify-between">
                          <span className="text-sm text-linera-gray-light">System Op</span>
                          <span className="text-white font-mono text-xs">{operation.system_operation_type}</span>
                        </div>
                      )}
                      {operation.application_id && (
                        <div className="space-y-1">
                          <span className="text-sm text-linera-gray-light">Application</span>
                          <CopyableHash value={operation.application_id} format="short" className="text-xs" />
                        </div>
                      )}
                    </div>
                  </div>
                  {operation.data && (
                    <BinaryDataSection 
                      data={operation.data} 
                      title="Operation Data"
                      maxDisplayBytes={100}
                    />
                  )}
                </div>
              ))}
            </div>
          </ExpandableSection>

          {/* Messages */}
          <ExpandableSection
            title="Messages"
            count={activity.messagesCount}
            icon={<Mail className="w-5 h-5 text-linera-red" />}
          >
            <div className="space-y-3">
              {messages.map((message) => (
                <div key={message.id} className="bg-linera-darker/30 border border-linera-border/50 rounded-lg p-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-linera-gray-light">Tx Index</span>
                        <span className="text-white font-medium">{message.transaction_index}</span>
                      </div>
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-linera-gray-light">Msg Index</span>
                        <span className="text-white font-medium">{message.message_index}</span>
                      </div>
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-linera-gray-light">Type</span>
                        <span className={`px-2 py-1 rounded text-xs font-medium ${
                          message.message_type === 'System' 
                            ? 'bg-blue-500/20 text-blue-400 border border-blue-500/30' 
                            : 'bg-purple-500/20 text-purple-400 border border-purple-500/30'
                        }`}>
                          {message.message_type}
                        </span>
                      </div>
                    </div>
                    <div className="space-y-2">
                      {message.system_message_type && (
                        <div className="flex items-center justify-between">
                          <span className="text-sm text-linera-gray-light">System Msg</span>
                          <span className="text-white font-mono text-xs">{message.system_message_type}</span>
                        </div>
                      )}
                      {message.system_amount !== undefined && message.system_amount !== null && (
                        <div className="flex items-center justify-between">
                          <span className="text-sm text-linera-gray-light">Amount</span>
                          <span className="text-white font-mono text-xs">{message.system_amount}</span>
                        </div>
                      )}
                      {message.system_target && (
                        <div className="space-y-1">
                          <span className="text-sm text-linera-gray-light">Target</span>
                          <CopyableHash value={message.system_target} format="short" className="text-xs" />
                        </div>
                      )}
                      {message.system_source && (
                        <div className="space-y-1">
                          <span className="text-sm text-linera-gray-light">Source</span>
                          <CopyableHash value={message.system_source} format="short" className="text-xs" />
                        </div>
                      )}
                      {message.system_owner && (
                        <div className="space-y-1">
                          <span className="text-sm text-linera-gray-light">Owner</span>
                          <CopyableHash value={message.system_owner} format="short" className="text-xs" />
                        </div>
                      )}
                      {message.system_recipient && (
                        <div className="space-y-1">
                          <span className="text-sm text-linera-gray-light">Recipient</span>
                          <CopyableHash value={message.system_recipient} format="short" className="text-xs" />
                        </div>
                      )}
                      {message.application_id && (
                        <div className="space-y-1">
                          <span className="text-sm text-linera-gray-light">Application</span>
                          <CopyableHash value={message.application_id} format="short" className="text-xs" />
                        </div>
                      )}
                      <div className="space-y-1">
                        <span className="text-sm text-linera-gray-light">Destination</span>
                        <CopyableHash value={message.destination_chain_id} format="short" className="text-xs" />
                      </div>
                    </div>
                  </div>
                  {message.data && (
                    <BinaryDataSection 
                      data={message.data} 
                      title="Message Data"
                      maxDisplayBytes={100}
                    />
                  )}
                </div>
              ))}
            </div>
          </ExpandableSection>

          {/* Events */}
          <ExpandableSection
            title="Events"
            count={activity.eventsCount}
            icon={<Zap className="w-5 h-5 text-linera-red" />}
          >
            <div className="space-y-3">
              {events.map((event) => (
                <div key={event.id} className="bg-linera-darker/30 border border-linera-border/50 rounded-lg p-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-linera-gray-light">Tx Index</span>
                        <span className="text-white font-medium">{event.transaction_index}</span>
                      </div>
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-linera-gray-light">Event Index</span>
                        <span className="text-white font-medium">{event.event_index}</span>
                      </div>
                      <div className="space-y-1">
                        <span className="text-sm text-linera-gray-light">Stream ID</span>
                        <CopyableHash value={event.stream_id} format="short" className="text-xs" />
                      </div>
                    </div>
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-linera-gray-light">Stream Index</span>
                        <span className="text-white font-mono text-xs">{event.stream_index}</span>
                      </div>
                    </div>
                  </div>
                  <BinaryDataSection 
                    data={event.data} 
                    title="Event Data"
                    maxDisplayBytes={100}
                  />
                </div>
              ))}
            </div>
          </ExpandableSection>

          {/* Oracle Responses */}
          <ExpandableSection
            title="Oracle Responses"
            count={activity.oracleResponsesCount}
            icon={<Database className="w-5 h-5 text-linera-red" />}
          >
            <div className="space-y-3">
              {oracleResponses.map((response) => (
                <div key={response.id} className="bg-linera-darker/30 border border-linera-border/50 rounded-lg p-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-linera-gray-light">Tx Index</span>
                        <span className="text-white font-medium">{response.transaction_index}</span>
                      </div>
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-linera-gray-light">Response Index</span>
                        <span className="text-white font-medium">{response.response_index}</span>
                      </div>
                      <div className="flex items-center justify-between">
                        <span className="text-sm text-linera-gray-light">Type</span>
                        <span className="px-2 py-1 rounded text-xs font-medium bg-green-500/20 text-green-400 border border-green-500/30">
                          {response.response_type}
                        </span>
                      </div>
                    </div>
                  </div>
                  {response.data && (
                    <BinaryDataSection 
                      data={response.data} 
                      title="Response Data"
                      maxDisplayBytes={100}
                    />
                  )}
                  {response.blob_hash && (
                    <div className="mt-3 pt-3 border-t border-linera-border/30">
                      <span className="text-sm text-linera-gray-light block mb-2">Blob Hash</span>
                      <CopyableHash value={response.blob_hash} format="short" className="text-xs" />
                    </div>
                  )}
                </div>
              ))}
            </div>
          </ExpandableSection>
        </>
      )}

      {/* Incoming Bundles */}
      {bundles.length > 0 && (
        <ExpandableSection
          title="Incoming Bundles"
          count={bundles.length}
          icon={<Package className="w-5 h-5 text-linera-red" />}
        >
          <div className="space-y-4">
            {bundles.map((bundle, index) => (
              <div
                key={bundle.id}
                className="block bg-linera-darker/30 border border-linera-border/50 rounded-lg p-6 animate-slide-up hover:bg-linera-darker/50 hover:border-linera-red/30 transition-all duration-300 group"
                style={{ animationDelay: `${index * 100}ms` }}
                onClick={(e) => e.stopPropagation()}
              >
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  <div className="space-y-4">
                    <div className="flex items-center justify-between">
                      <span className="text-sm text-linera-gray-light">Bundle Index</span>
                      <span className="font-semibold text-white">{bundle.bundle_index}</span>
                    </div>
                    
                    <div className="space-y-2">
                      <span className="text-sm text-linera-gray-light">Origin Chain</span>
                      <CopyableHash value={bundle.origin_chain_id} format="short" />
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
                    
                    <div className="flex items-center justify-between">
                      <span className="text-sm text-linera-gray-light">Timestamp</span>
                      <span className="text-white font-medium text-sm">
                        {new Date(bundle.source_timestamp / 1000).toLocaleString()}
                      </span>
                    </div>
                  </div>
                </div>

                {/* Posted Messages */}
                {bundle.messages && bundle.messages.length > 0 && (
                  <div className="mt-4 pt-4 border-t border-linera-border/30">
                    <div className="flex items-center space-x-2 mb-3">
                      <MessageCircle className="w-4 h-4 text-linera-gray-medium" />
                      <span className="text-sm text-linera-gray-light">Posted Messages</span>
                      <span className="px-2 py-0.5 bg-linera-darker rounded text-xs text-white">
                        {bundle.messages.length}
                      </span>
                    </div>
                    <div className="space-y-2">
                      {bundle.messages.map((msg) => (
                        <div key={msg.id} className="bg-linera-darker/50 rounded p-3 text-xs">
                          <div className="flex items-center justify-between mb-2">
                            <span className="text-linera-gray-light">#{msg.message_index}</span>
                            <div className="flex items-center space-x-2">
                              {msg.message_type && (
                                <span className={`px-1.5 py-0.5 rounded text-xs ${
                                  msg.message_type === 'System' 
                                    ? 'bg-blue-500/20 text-blue-400 border border-blue-500/30' 
                                    : 'bg-purple-500/20 text-purple-400 border border-purple-500/30'
                                }`}>
                                  {msg.message_type}
                                </span>
                              )}
                              <span className="text-linera-gray-medium">{msg.message_kind}</span>
                            </div>
                          </div>
                          
                          {/* System Message Details */}
                          {msg.system_message_type && (
                            <div className="space-y-1 mt-2">
                              <div className="flex items-center justify-between">
                                <span className="text-linera-gray-light">Type:</span>
                                <span className="text-white">{msg.system_message_type}</span>
                              </div>
                              {msg.system_amount !== undefined && msg.system_amount !== null && (
                                <div className="flex items-center justify-between">
                                  <span className="text-linera-gray-light">Amount:</span>
                                  <span className="text-white font-mono">{msg.system_amount}</span>
                                </div>
                              )}
                              {msg.system_target && (
                                <div className="flex items-center justify-between">
                                  <span className="text-linera-gray-light">Target:</span>
                                  <CopyableHash value={msg.system_target} format="short" className="text-xs"/>
                                </div>
                              )}
                              {msg.system_source && (
                                <div className="flex items-center justify-between">
                                  <span className="text-linera-gray-light">Source:</span>
                                  <CopyableHash value={msg.system_source} format="short" className="text-xs"/>
                                </div>
                              )}
                              {msg.system_owner && (
                                <div className="flex items-center justify-between">
                                  <span className="text-linera-gray-light">Owner:</span>
                                  <CopyableHash value={msg.system_owner} format="short" className="text-xs"/>
                                </div>
                              )}
                              {msg.system_recipient && (
                                <div className="flex items-center justify-between">
                                  <span className="text-linera-gray-light">Recipient:</span>
                                  <CopyableHash value={msg.system_recipient} format="short" className="text-xs"/>
                                </div>
                              )}
                            </div>
                          )}
                          
                          {/* User Message Details */}
                          {msg.application_id && (
                            <div className="mt-2">
                              <div className="flex items-center justify-between">
                                <span className="text-linera-gray-light">Application:</span>
                                <CopyableHash value={msg.application_id} format="short" className="text-xs" />
                              </div>
                            </div>
                          )}
                          
                          {/* Grant Amount */}
                          {msg.grant_amount != '0' && (
                            <div className="mt-2 flex items-center justify-between">
                              <span className="text-linera-gray-light">Grant:</span>
                              <span className="text-white font-mono">{msg.grant_amount}</span>
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Source Block Info */}
                <div className="mt-4 pt-4 border-t border-linera-border/30">
                  <div className="flex items-center justify-between mb-3">
                    <span className="text-sm text-linera-gray-light">Source Block</span>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        navigate(`/block/${bundle.source_cert_hash}`);
                      }}
                      className="flex items-center space-x-2 text-linera-gray-medium hover:text-linera-red transition-colors"
                    >
                      <span className="text-sm">View Source Block</span>
                      <ChevronRight className="w-4 h-4 hover:translate-x-1 transition-transform" />
                    </button>
                  </div>
                  <CopyableHash value={bundle.source_cert_hash} format="short" />
                </div>
              </div>
            ))}
          </div>
        </ExpandableSection>
      )}

      {/* Raw Data Preview - Only show if no structured data is available */}
      {(!activity || (activity.operationsCount === 0 && activity.messagesCount === 0 && activity.eventsCount === 0 && activity.oracleResponsesCount === 0)) && (
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
          
          <BinaryDataSection 
            data={block.data} 
            title="Block Data"
            maxDisplayBytes={400}
            className="mt-0 pt-0 border-t-0"
          />
        </div>
      )}
    </div>
  );
};