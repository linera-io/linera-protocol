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

  // Get posted messages for a bundle with SystemMessage fields
  getPostedMessages(bundleId) {
    const stmt = this.db.prepare(`
      SELECT *, system_target, system_amount, system_source, system_owner, system_recipient,
             message_type, application_id, system_message_type
      FROM posted_messages 
      WHERE bundle_id = ? 
      ORDER BY message_index ASC
    `);
    return stmt.all(bundleId);
  }

  // Get block with all bundles and their messages in a single query
  getBlockWithBundlesAndMessages(blockHash) {
    const stmt = this.db.prepare(`
      SELECT 
        ib.id as bundle_id,
        ib.bundle_index,
        ib.origin_chain_id,
        ib.action,
        ib.source_height,
        ib.source_timestamp,
        ib.source_cert_hash,
        ib.transaction_index as bundle_transaction_index,
        ib.created_at as bundle_created_at,
        pm.id as message_id,
        pm.message_index,
        pm.authenticated_signer,
        pm.grant_amount,
        pm.refund_grant_to,
        pm.message_kind,
        pm.message_type,
        pm.application_id,
        pm.system_message_type,
        pm.system_target,
        pm.system_amount,
        pm.system_source,
        pm.system_owner,
        pm.system_recipient,
        pm.message_data,
        pm.created_at as message_created_at
      FROM incoming_bundles ib
      LEFT JOIN posted_messages pm ON ib.id = pm.bundle_id
      WHERE ib.block_hash = ?
      ORDER BY ib.bundle_index, pm.message_index
    `);
    
    const rows = stmt.all(blockHash);
    
    // Group results by bundle
    const bundlesMap = new Map();
    
    for (const row of rows) {
      const bundleId = row.bundle_id;
      
      if (!bundlesMap.has(bundleId)) {
        bundlesMap.set(bundleId, {
          id: bundleId,
          block_hash: blockHash,
          bundle_index: row.bundle_index,
          origin_chain_id: row.origin_chain_id,
          action: row.action,
          source_height: row.source_height,
          source_timestamp: row.source_timestamp,
          source_cert_hash: row.source_cert_hash,
          transaction_index: row.bundle_transaction_index,
          created_at: row.bundle_created_at,
          messages: []
        });
      }
      
      // Add message if it exists (LEFT JOIN may produce NULL messages)
      if (row.message_id) {
        const bundle = bundlesMap.get(bundleId);
        bundle.messages.push({
          id: row.message_id,
          bundle_id: bundleId,
          message_index: row.message_index,
          authenticated_signer: row.authenticated_signer,
          grant_amount: row.grant_amount,
          refund_grant_to: row.refund_grant_to,
          message_kind: row.message_kind,
          message_type: row.message_type,
          application_id: row.application_id,
          system_message_type: row.system_message_type,
          system_target: row.system_target,
          system_amount: row.system_amount,
          system_source: row.system_source,
          system_owner: row.system_owner,
          system_recipient: row.system_recipient,
          message_data: row.message_data ? Buffer.from(row.message_data).toString('base64') : null,
          created_at: row.message_created_at
        });
      }
    }
    
    return Array.from(bundlesMap.values());
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