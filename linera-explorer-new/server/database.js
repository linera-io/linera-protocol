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

  // Get operations for a block
  getOperations(blockHash) {
    const stmt = this.db.prepare(`
      SELECT * FROM operations 
      WHERE block_hash = ? 
      ORDER BY operation_index ASC
    `);
    const operations = stmt.all(blockHash);
    
    // Convert binary data to base64 for JSON transport
    return operations.map(op => ({
      ...op,
      data: op.data ? Buffer.from(op.data).toString('base64') : null
    }));
  }

  // Get messages for a block
  getMessages(blockHash) {
    const stmt = this.db.prepare(`
      SELECT *, system_target, system_amount, system_source, system_owner, system_recipient FROM outgoing_messages 
      WHERE block_hash = ? 
      ORDER BY transaction_index ASC, message_index ASC
    `);
    const messages = stmt.all(blockHash);
    
    // Convert binary data to base64 for JSON transport
    return messages.map(msg => ({
      ...msg,
      data: msg.data ? Buffer.from(msg.data).toString('base64') : null
    }));
  }

  // Get events for a block
  getEvents(blockHash) {
    const stmt = this.db.prepare(`
      SELECT * FROM events 
      WHERE block_hash = ? 
      ORDER BY transaction_index ASC, event_index ASC
    `);
    const events = stmt.all(blockHash);
    
    // Convert binary data to base64 for JSON transport
    return events.map(event => ({
      ...event,
      data: event.data ? Buffer.from(event.data).toString('base64') : null
    }));
  }

  // Get oracle responses for a block
  getOracleResponses(blockHash) {
    const stmt = this.db.prepare(`
      SELECT * FROM oracle_responses 
      WHERE block_hash = ? 
      ORDER BY transaction_index ASC, response_index ASC
    `);
    const responses = stmt.all(blockHash);
    
    // Convert binary data to base64 for JSON transport
    return responses.map(resp => ({
      ...resp,
      data: resp.data ? Buffer.from(resp.data).toString('base64') : null
    }));
  }


  close() {
    this.db.close();
  }
}