import pg from 'pg';
const { Pool } = pg;

export class BlockchainDatabasePostgres {
  constructor(connectionString) {
    this.pool = new Pool({
      connectionString,
      // Connection pool settings
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });
  }

  // Get all blocks with pagination
  async getBlocks(limit = 50, offset = 0) {
    const result = await this.pool.query(
      `SELECT hash, chain_id, height, timestamp, LENGTH(data) as size
       FROM blocks
       ORDER BY timestamp DESC
       LIMIT $1 OFFSET $2`,
      [limit, offset]
    );
    return result.rows;
  }

  // Get block by hash
  async getBlockByHash(hash) {
    const result = await this.pool.query(
      `SELECT hash, chain_id, height, data, timestamp
       FROM blocks
       WHERE hash = $1`,
      [hash]
    );
    return result.rows[0] || null;
  }

  // Get blocks for a specific chain
  async getBlocksByChain(chainId, limit = 50, offset = 0) {
    const result = await this.pool.query(
      `SELECT hash, chain_id, height, timestamp, LENGTH(data) as size
       FROM blocks
       WHERE chain_id = $1
       ORDER BY height DESC
       LIMIT $2 OFFSET $3`,
      [chainId, limit, offset]
    );
    return result.rows;
  }

  // Get latest block for a chain
  async getLatestBlockForChain(chainId) {
    const result = await this.pool.query(
      `SELECT hash, chain_id, height, timestamp, LENGTH(data) as size
       FROM blocks
       WHERE chain_id = $1
       ORDER BY height DESC
       LIMIT 1`,
      [chainId]
    );
    return result.rows[0] || null;
  }

  // Get incoming bundles for a block
  async getIncomingBundles(blockHash) {
    const result = await this.pool.query(
      `SELECT * FROM incoming_bundles
       WHERE block_hash = $1
       ORDER BY bundle_index ASC`,
      [blockHash]
    );
    return result.rows;
  }

  // Get posted messages for a bundle with SystemMessage fields
  async getPostedMessages(bundleId) {
    const result = await this.pool.query(
      `SELECT *, system_target, system_amount, system_source, system_owner, system_recipient,
              message_type, application_id, system_message_type
       FROM posted_messages
       WHERE bundle_id = $1
       ORDER BY message_index ASC`,
      [bundleId]
    );
    return result.rows;
  }

  // Get block with all bundles and their messages in a single query
  async getBlockWithBundlesAndMessages(blockHash) {
    const result = await this.pool.query(
      `SELECT
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
      WHERE ib.block_hash = $1
      ORDER BY ib.bundle_index, pm.message_index`,
      [blockHash]
    );

    const rows = result.rows;

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
  async getChains(limit = null, offset = 0) {
    let query = `
      SELECT
        chain_id,
        COUNT(*) as block_count,
        MAX(height) as latest_height,
        (SELECT hash FROM blocks b2 WHERE b2.chain_id = b1.chain_id ORDER BY height DESC LIMIT 1) as latest_block_hash
      FROM blocks b1
      GROUP BY chain_id
      ORDER BY latest_height DESC
    `;

    const params = [];
    if (limit !== null) {
      query += ` LIMIT $1 OFFSET $2`;
      params.push(limit, offset);
    }

    const result = await this.pool.query(query, params);
    return result.rows;
  }

  // Get total chain count
  async getChainsCount() {
    const result = await this.pool.query(
      `SELECT COUNT(DISTINCT chain_id) as count FROM blocks`
    );
    return result.rows[0].count;
  }

  // Search chains by chain ID
  async getChainById(chainId) {
    const result = await this.pool.query(
      `SELECT
        chain_id,
        COUNT(*) as block_count,
        MAX(height) as latest_height,
        (SELECT hash FROM blocks b2 WHERE b2.chain_id = b1.chain_id ORDER BY height DESC LIMIT 1) as latest_block_hash
      FROM blocks b1
      WHERE chain_id = $1
      GROUP BY chain_id`,
      [chainId]
    );
    return result.rows[0] || null;
  }

  // Get total block count
  async getTotalBlockCount() {
    const result = await this.pool.query('SELECT COUNT(*) as count FROM blocks');
    return result.rows[0].count;
  }

  // Get block count for a specific chain
  async getChainBlockCount(chainId) {
    const result = await this.pool.query(
      `SELECT COUNT(*) as count FROM blocks WHERE chain_id = $1`,
      [chainId]
    );
    return result.rows[0].count;
  }

  // Get operations for a block
  async getOperations(blockHash) {
    const result = await this.pool.query(
      `SELECT * FROM operations
       WHERE block_hash = $1
       ORDER BY operation_index ASC`,
      [blockHash]
    );

    // Convert binary data to base64 for JSON transport
    return result.rows.map(op => ({
      ...op,
      data: op.data ? Buffer.from(op.data).toString('base64') : null
    }));
  }

  // Get messages for a block
  async getMessages(blockHash) {
    const result = await this.pool.query(
      `SELECT *, system_target, system_amount, system_source, system_owner, system_recipient
       FROM outgoing_messages
       WHERE block_hash = $1
       ORDER BY transaction_index ASC, message_index ASC`,
      [blockHash]
    );

    // Convert binary data to base64 for JSON transport
    return result.rows.map(msg => ({
      ...msg,
      data: msg.data ? Buffer.from(msg.data).toString('base64') : null
    }));
  }

  // Get events for a block
  async getEvents(blockHash) {
    const result = await this.pool.query(
      `SELECT * FROM events
       WHERE block_hash = $1
       ORDER BY transaction_index ASC, event_index ASC`,
      [blockHash]
    );

    // Convert binary data to base64 for JSON transport
    return result.rows.map(event => ({
      ...event,
      data: event.data ? Buffer.from(event.data).toString('base64') : null
    }));
  }

  // Get oracle responses for a block
  async getOracleResponses(blockHash) {
    const result = await this.pool.query(
      `SELECT * FROM oracle_responses
       WHERE block_hash = $1
       ORDER BY transaction_index ASC, response_index ASC`,
      [blockHash]
    );

    // Convert binary data to base64 for JSON transport
    return result.rows.map(resp => ({
      ...resp,
      data: resp.data ? Buffer.from(resp.data).toString('base64') : null
    }));
  }

  async close() {
    await this.pool.end();
  }
}
