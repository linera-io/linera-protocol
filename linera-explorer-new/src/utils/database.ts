import { Block, BlockInfo, IncomingBundle, PostedMessage, ChainInfo, Operation, Message, Event, OracleResponse, IncomingBundleWithMessages } from '../types/blockchain';

// Use environment variable if set (for production), otherwise use relative path (for dev with Vite proxy)
export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api';

export class BlockchainAPI {
  // Get all blocks with pagination
  async getBlocks(limit: number = 50, offset: number = 0): Promise<BlockInfo[]> {
    const response = await fetch(`${API_BASE_URL}/blocks?limit=${limit}&offset=${offset}`);
    if (!response.ok) {
      throw new Error(`Failed to fetch blocks: ${response.statusText}`);
    }
    return response.json();
  }

  // Get block by hash
  async getBlockByHash(hash: string): Promise<Block | null> {
    const response = await fetch(`${API_BASE_URL}/blocks/${hash}`);
    if (response.status === 404) {
      return null;
    }
    if (!response.ok) {
      throw new Error(`Failed to fetch block: ${response.statusText}`);
    }
    const block = await response.json();
    
    if (block.data) {
      block.data = this.base64ToUint8Array(block.data);
    }
    
    return block;
  }

  // Get blocks for a specific chain
  async getBlocksByChain(chainId: string, limit: number = 50, offset: number = 0): Promise<BlockInfo[]> {
    const response = await fetch(`${API_BASE_URL}/chains/${encodeURIComponent(chainId)}/blocks?limit=${limit}&offset=${offset}`);
    if (!response.ok) {
      throw new Error(`Failed to fetch chain blocks: ${response.statusText}`);
    }
    return response.json();
  }

  // Get latest block for a chain
  async getLatestBlockForChain(chainId: string): Promise<BlockInfo | null> {
    const blocks = await this.getBlocksByChain(chainId, 1, 0);
    return blocks.length > 0 ? blocks[0] : null;
  }

  // Get incoming bundles for a block
  async getIncomingBundles(blockHash: string): Promise<IncomingBundle[]> {
    const response = await fetch(`${API_BASE_URL}/blocks/${blockHash}/bundles`);
    if (!response.ok) {
      throw new Error(`Failed to fetch bundles: ${response.statusText}`);
    }
    return response.json();
  }

  // Get bundles with messages - optimized single query
  async getBundlesWithMessages(blockHash: string): Promise<IncomingBundleWithMessages[]> {
    const response = await fetch(`${API_BASE_URL}/blocks/${blockHash}/bundles-with-messages`);
    if (!response.ok) {
      throw new Error(`Failed to fetch bundles with messages: ${response.statusText}`);
    }
    const data = await response.json();
    
    // Convert base64 data to Uint8Array for binary fields
    return data.map((bundle: any) => ({
      ...bundle,
      messages: bundle.messages.map((msg: any) => ({
        ...msg,
        authenticated_owner: msg.authenticated_owner ?
          this.base64ToUint8Array(msg.authenticated_owner) : new Uint8Array(),
        refund_grant_to: msg.refund_grant_to ? 
          this.base64ToUint8Array(msg.refund_grant_to) : new Uint8Array(),
        message_data: this.base64ToUint8Array(msg.message_data)
      }))
    }));
  }

  // Get posted messages for a bundle
  async getPostedMessages(bundleId: number): Promise<PostedMessage[]> {
    const response = await fetch(`${API_BASE_URL}/bundles/${bundleId}/messages`);
    if (!response.ok) {
      throw new Error(`Failed to fetch messages: ${response.statusText}`);
    }
    const messages = await response.json();

    messages.forEach((message: any) => {
      if (message.authenticated_owner) {
        message.authenticated_owner = this.base64ToUint8Array(message.authenticated_owner);
      }
      if (message.refund_grant_to) {
        message.refund_grant_to = this.base64ToUint8Array(message.refund_grant_to);
      }
      if (message.message_data) {
        message.message_data = this.base64ToUint8Array(message.message_data);
      }
    });
    
    return messages;
  }

  // Get all unique chains with stats
  async getChains(): Promise<ChainInfo[]> {
    const response = await fetch(`${API_BASE_URL}/chains`);
    if (!response.ok) {
      throw new Error(`Failed to fetch chains: ${response.statusText}`);
    }
    return response.json();
  }

  // Search blocks by hash prefix
  async searchBlocks(query: string): Promise<BlockInfo[]> {
    const response = await fetch(`${API_BASE_URL}/search?q=${encodeURIComponent(query)}`);
    if (!response.ok) {
      throw new Error(`Failed to search blocks: ${response.statusText}`);
    }
    return response.json();
  }

  // Get total block count
  async getTotalBlockCount(): Promise<number> {
    const response = await fetch(`${API_BASE_URL}/stats`);
    if (!response.ok) {
      throw new Error(`Failed to fetch stats: ${response.statusText}`);
    }
    const stats = await response.json();
    return stats.totalBlocks;
  }

  // Get operations for a block
  async getOperations(blockHash: string): Promise<Operation[]> {
    const response = await fetch(`${API_BASE_URL}/blocks/${blockHash}/operations`);
    if (!response.ok) {
      throw new Error(`Failed to fetch operations: ${response.statusText}`);
    }
    const operations = await response.json();
    
    // Convert binary data from base64
    return operations.map((op: any) => ({
      ...op,
      data: op.data ? this.base64ToUint8Array(op.data) : new Uint8Array()
    }));
  }

  // Get messages for a block
  async getMessages(blockHash: string): Promise<Message[]> {
    const response = await fetch(`${API_BASE_URL}/blocks/${blockHash}/messages`);
    if (!response.ok) {
      throw new Error(`Failed to fetch messages: ${response.statusText}`);
    }
    const messages = await response.json();
    
    // Convert binary data from base64
    return messages.map((msg: any) => ({
      ...msg,
      data: msg.data ? this.base64ToUint8Array(msg.data) : new Uint8Array()
    }));
  }

  // Get events for a block
  async getEvents(blockHash: string): Promise<Event[]> {
    const response = await fetch(`${API_BASE_URL}/blocks/${blockHash}/events`);
    if (!response.ok) {
      throw new Error(`Failed to fetch events: ${response.statusText}`);
    }
    const events = await response.json();
    
    // Convert binary data from base64
    return events.map((event: any) => ({
      ...event,
      data: event.data ? this.base64ToUint8Array(event.data) : new Uint8Array()
    }));
  }

  // Get oracle responses for a block
  async getOracleResponses(blockHash: string): Promise<OracleResponse[]> {
    const response = await fetch(`${API_BASE_URL}/blocks/${blockHash}/oracle-responses`);
    if (!response.ok) {
      throw new Error(`Failed to fetch oracle responses: ${response.statusText}`);
    }
    const responses = await response.json();
    
    // Convert binary data from base64
    return responses.map((resp: any) => ({
      ...resp,
      data: resp.data ? this.base64ToUint8Array(resp.data) : new Uint8Array(),
    }));
  }

  // Helper method to convert base64 to Uint8Array
  // PostgreSQL's ENCODE adds newlines every 76 chars, strip them for atob()
  private base64ToUint8Array(base64: string): Uint8Array {
    const binaryString = atob(base64.replace(/\n/g, ''));
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
      bytes[i] = binaryString.charCodeAt(i);
    }
    return bytes;
  }
}