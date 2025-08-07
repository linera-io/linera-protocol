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
  grant_amount: number;
  refund_grant_to: Uint8Array | null;
  message_kind: string;
  message_data: Uint8Array;
  created_at: string;
}

export interface ChainInfo {
  chain_id: string;
  block_count: number;
  latest_height: number;
  latest_block_hash: string;
}