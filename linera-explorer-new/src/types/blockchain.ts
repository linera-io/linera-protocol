export interface Block {
  hash: string;
  chain_id: string;
  height: number;
  timestamp: number;
  data: Uint8Array;
  created_at: string;
}

export interface BlockInfo {
  hash: string;
  chain_id: string;
  height: number;
  timestamp: number;
  created_at: string;
  size: number;
}

export interface Blob {
  hash: string;
  type: string;
  data: Uint8Array;
  created_at: string;
}

export interface IncomingBundle {
  id: number;
  block_hash: string;
  bundle_index: number;
  origin_chain_id: string;
  action: 'Accept' | 'Reject';
  source_height: number;
  source_timestamp: number;
  source_cert_hash: string;
  transaction_index: number;
  created_at: string;
}

export interface PostedMessage {
  id: number;
  bundle_id: number;
  message_index: number;
  authenticated_signer: Uint8Array | null;
  grant_amount: string;
  refund_grant_to: Uint8Array | null;
  message_kind: string;
  message_type?: 'System' | 'User';
  application_id?: string;
  system_message_type?: string;
  system_target?: string;
  system_amount?: string;
  system_source?: string;
  system_owner?: string;
  system_recipient?: string;
  message_data: Uint8Array;
  created_at: string;
}

export interface IncomingBundleWithMessages extends IncomingBundle {
  messages: PostedMessage[];
}

export interface ChainInfo {
  chain_id: string;
  block_count: number;
  latest_height: number;
  latest_block_hash: string;
}

// Denormalized data structures
export interface Operation {
  id: number;
  block_hash: string;
  operation_index: number;
  operation_type: 'System' | 'User';
  application_id?: string;
  system_operation_type?: string;
  authenticated_signer?: string;
  data: Uint8Array;
  created_at: string;
}

export interface Message {
  id: number;
  block_hash: string;
  transaction_index: number;
  message_index: number;
  destination_chain_id: string;
  authenticated_signer?: string;
  grant_amount: string;
  message_kind: string; // 'Simple', 'Tracked', 'Bouncing', 'Protected'
  message_type: 'System' | 'User';
  application_id?: string;
  system_message_type?: string;
  system_target?: string;
  system_amount?: string;
  system_source?: string;
  system_owner?: string;
  system_recipient?: string;
  data: Uint8Array;
  created_at: string;
}

export interface Event {
  id: number;
  block_hash: string;
  transaction_index: number;
  event_index: number;
  stream_id: string;
  stream_index: number;
  data: Uint8Array;
  created_at: string;
}

export interface OracleResponse {
  id: number;
  block_hash: string;
  transaction_index: number;
  response_index: number;
  response_type: string;
  blob_hash?: string;
  data?: Uint8Array;
  created_at: string;
}