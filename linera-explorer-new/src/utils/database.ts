import { Block, BlockInfo, IncomingBundle, PostedMessage, ChainInfo } from '../types/blockchain';

const API_BASE_URL = 'http://localhost:3002/api';

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
    
    // Convert base64 data back to Uint8Array
    if (block.data) {
      const binaryString = atob(block.data);
      const bytes = new Uint8Array(binaryString.length);
      for (let i = 0; i < binaryString.length; i++) {
        bytes[i] = binaryString.charCodeAt(i);
      }
      block.data = bytes;
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

  // Get posted messages for a bundle
  async getPostedMessages(bundleId: number): Promise<PostedMessage[]> {
    const response = await fetch(`${API_BASE_URL}/bundles/${bundleId}/messages`);
    if (!response.ok) {
      throw new Error(`Failed to fetch messages: ${response.statusText}`);
    }
    const messages = await response.json();
    
    // Convert base64 data back to Uint8Array
    const base64ToUint8Array = (base64: string): Uint8Array => {
      const binaryString = atob(base64);
      const bytes = new Uint8Array(binaryString.length);
      for (let i = 0; i < binaryString.length; i++) {
        bytes[i] = binaryString.charCodeAt(i);
      }
      return bytes;
    };

    messages.forEach((message: any) => {
      if (message.authenticated_signer) {
        message.authenticated_signer = base64ToUint8Array(message.authenticated_signer);
      }
      if (message.refund_grant_to) {
        message.refund_grant_to = base64ToUint8Array(message.refund_grant_to);
      }
      if (message.message_data) {
        message.message_data = base64ToUint8Array(message.message_data);
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
}