import Database from 'better-sqlite3';

export class BlockchainDatabase {
  constructor(dbPath) {
    this.db = new Database(dbPath, { readonly: true });
  }

  // Get all blocks with pagination
  getBlocks(limit = 50, offset = 0) {
    const stmt = this.db.prepare(`
      SELECT hash, chain_id, height, timestamp, LENGTH(data) as size 
      FROM blocks 
      ORDER BY timestamp DESC 
      LIMIT ? OFFSET ?
    `);
    return stmt.all(limit, offset);
  }

  // Get block by hash
  getBlockByHash(hash) {
    const stmt = this.db.prepare(`
      SELECT hash, chain_id, height, data, timestamp 
      FROM blocks 
      WHERE hash = ?
    `);
    return stmt.get(hash);
  }

  // Get blocks for a specific chain
  getBlocksByChain(chainId, limit = 50, offset = 0) {
    const stmt = this.db.prepare(`
      SELECT hash, chain_id, height, timestamp, LENGTH(data) as size 
      FROM blocks 
      WHERE chain_id = ? 
      ORDER BY height DESC 
      LIMIT ? OFFSET ?
    `);
    return stmt.all(chainId, limit, offset);
  }

  // Get latest block for a chain
  getLatestBlockForChain(chainId) {
    const stmt = this.db.prepare(`
      SELECT hash, chain_id, height, timestamp, LENGTH(data) as size 
      FROM blocks 
      WHERE chain_id = ? 
      ORDER BY height DESC 
      LIMIT 1
    `);
    return stmt.get(chainId);
  }

  // Get incoming bundles for a block
  getIncomingBundles(blockHash) {
    const stmt = this.db.prepare(`
      SELECT * FROM incoming_bundles 
      WHERE block_hash = ? 
      ORDER BY bundle_index ASC
    `);
    return stmt.all(blockHash);
  }

  // Get posted messages for a bundle
  getPostedMessages(bundleId) {
    const stmt = this.db.prepare(`
      SELECT * FROM posted_messages 
      WHERE bundle_id = ? 
      ORDER BY message_index ASC
    `);
    return stmt.all(bundleId);
  }

  // Get all unique chains with stats
  getChains() {
    const stmt = this.db.prepare(`
      SELECT 
        chain_id,
        COUNT(*) as block_count,
        MAX(height) as latest_height,
        (SELECT hash FROM blocks b2 WHERE b2.chain_id = b1.chain_id ORDER BY height DESC LIMIT 1) as latest_block_hash
      FROM blocks b1 
      GROUP BY chain_id 
      ORDER BY latest_height DESC
    `);
    return stmt.all();
  }

  // Search blocks by hash prefix
  searchBlocks(query) {
    const stmt = this.db.prepare(`
      SELECT hash, chain_id, height, timestamp, LENGTH(data) as size 
      FROM blocks 
      WHERE hash = ? 
      ORDER BY timestamp DESC 
      LIMIT 20
    `);
    return stmt.all(`${query}%`);
  }

  // Get total block count
  getTotalBlockCount() {
    const stmt = this.db.prepare('SELECT COUNT(*) as count FROM blocks');
    const result = stmt.get();
    return result.count;
  }

  close() {
    this.db.close();
  }
}