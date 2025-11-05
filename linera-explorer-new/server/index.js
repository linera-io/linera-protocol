import express from 'express';
import cors from 'cors';
import { BlockchainDatabase } from './database.js';

const app = express();
const PORT = process.env.PORT || 3002;

// Middleware
app.use(cors());
app.use(express.json());

// Database connection
const dbPath = process.env.DB_PATH || '../docker/indexer-data/indexer.db';
let db;

try {
  console.log(`Connecting to database at: ${dbPath}`);
  db = new BlockchainDatabase(dbPath);
  console.log(`Connected to database at: ${dbPath}`);
} catch (error) {
  console.error('Failed to connect to database:', error.message);
  console.log('Make sure the database path is correct and the file exists.');
  process.exit(1);
}

// Error handler
const handleError = (res, error, message = 'Internal server error') => {
  console.error(error);
  res.status(500).json({ error: message });
};

// Routes

// Get blocks with pagination
app.get('/api/blocks', (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 50;
    const offset = parseInt(req.query.offset) || 0;
    
    const blocks = db.getBlocks(limit, offset);
    res.json(blocks);
  } catch (error) {
    handleError(res, error, 'Failed to fetch blocks');
  }
});

// Get block by hash
app.get('/api/blocks/:hash', (req, res) => {
  try {
    const { hash } = req.params;
    const block = db.getBlockByHash(hash);
    
    if (!block) {
      return res.status(404).json({ error: 'Block not found' });
    }
    
    // Convert binary data to base64 for JSON transport
    if (block.data) {
      block.data = Buffer.from(block.data).toString('base64');
    }
    
    res.json(block);
  } catch (error) {
    handleError(res, error, 'Failed to fetch block');
  }
});

// Get incoming bundles for a block
app.get('/api/blocks/:hash/bundles', (req, res) => {
  try {
    const { hash } = req.params;
    const bundles = db.getIncomingBundles(hash);
    res.json(bundles);
  } catch (error) {
    handleError(res, error, 'Failed to fetch bundles');
  }
});

// Get bundles with messages - optimized single query
app.get('/api/blocks/:hash/bundles-with-messages', (req, res) => {
  try {
    const { hash } = req.params;
    const bundlesWithMessages = db.getBlockWithBundlesAndMessages(hash);
    
    // Convert binary data to base64 for JSON transport
    bundlesWithMessages.forEach(bundle => {
      bundle.messages.forEach(message => {
        if (message.authenticated_signer) {
          message.authenticated_signer = Buffer.from(message.authenticated_signer).toString('base64');
        }
        if (message.refund_grant_to) {
          message.refund_grant_to = Buffer.from(message.refund_grant_to).toString('base64');
        }
      });
    });
    
    res.json(bundlesWithMessages);
  } catch (error) {
    handleError(res, error, 'Failed to fetch bundles with messages');
  }
});

// Get posted messages for a bundle
app.get('/api/bundles/:id/messages', (req, res) => {
  try {
    const bundleId = parseInt(req.params.id);
    const messages = db.getPostedMessages(bundleId);
    
    // Convert binary data to base64 for JSON transport
    messages.forEach(message => {
      if (message.authenticated_signer) {
        message.authenticated_signer = Buffer.from(message.authenticated_signer).toString('base64');
      }
      if (message.refund_grant_to) {
        message.refund_grant_to = Buffer.from(message.refund_grant_to).toString('base64');
      }
      if (message.message_data) {
        message.message_data = Buffer.from(message.message_data).toString('base64');
      }
    });
    
    res.json(messages);
  } catch (error) {
    handleError(res, error, 'Failed to fetch messages');
  }
});

// Get all chains
app.get('/api/chains', (req, res) => {
  try {
    const limit = req.query.limit ? parseInt(req.query.limit) : null;
    const offset = parseInt(req.query.offset) || 0;

    const chains = db.getChains(limit, offset);
    res.json(chains);
  } catch (error) {
    handleError(res, error, 'Failed to fetch chains');
  }
});

// Get total chain count
app.get('/api/chains/count', (req, res) => {
  try {
    const count = db.getChainsCount();
    res.json({ count });
  } catch (error) {
    handleError(res, error, 'Failed to fetch chain count');
  }
});

// Get chain by ID
app.get('/api/chains/:chainId', (req, res) => {
  try {
    const { chainId } = req.params;

    // Validate hex string format (64 chars)
    if (!/^[0-9a-f]{64}$/i.test(chainId)) {
      return res.status(400).json({ error: 'Chain ID must be a 64-character hex string' });
    }

    const chain = db.getChainById(chainId);

    if (!chain) {
      return res.status(404).json({ error: 'Chain not found' });
    }

    res.json(chain);
  } catch (error) {
    handleError(res, error, 'Failed to fetch chain');
  }
});

// Get blocks for a specific chain
app.get('/api/chains/:chainId/blocks', (req, res) => {
  try {
    const { chainId } = req.params;
    const limit = parseInt(req.query.limit) || 50;
    const offset = parseInt(req.query.offset) || 0;
    
    const blocks = db.getBlocksByChain(chainId, limit, offset);
    res.json(blocks);
  } catch (error) {
    handleError(res, error, 'Failed to fetch chain blocks');
  }
});

// Get database stats
app.get('/api/stats', (req, res) => {
  try {
    const totalBlocks = db.getTotalBlockCount();
    const chains = db.getChains();
    
    res.json({
      totalBlocks,
      totalChains: chains.length,
      chains: chains.slice(0, 5) // Top 5 chains
    });
  } catch (error) {
    handleError(res, error, 'Failed to fetch stats');
  }
});

// Get operations for a block
app.get('/api/blocks/:hash/operations', (req, res) => {
  try {
    const { hash } = req.params;
    const operations = db.getOperations(hash);
    res.json(operations);
  } catch (error) {
    handleError(res, error, 'Failed to fetch operations');
  }
});

// Get messages for a block
app.get('/api/blocks/:hash/messages', (req, res) => {
  try {
    const { hash } = req.params;
    const messages = db.getMessages(hash);
    res.json(messages);
  } catch (error) {
    handleError(res, error, 'Failed to fetch messages');
  }
});

// Get events for a block
app.get('/api/blocks/:hash/events', (req, res) => {
  try {
    const { hash } = req.params;
    const events = db.getEvents(hash);
    res.json(events);
  } catch (error) {
    handleError(res, error, 'Failed to fetch events');
  }
});

// Get oracle responses for a block
app.get('/api/blocks/:hash/oracle-responses', (req, res) => {
  try {
    const { hash } = req.params;
    const oracleResponses = db.getOracleResponses(hash);
    res.json(oracleResponses);
  } catch (error) {
    handleError(res, error, 'Failed to fetch oracle responses');
  }
});


// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('Shutting down server...');
  if (db) {
    db.close();
  }
  process.exit(0);
});

app.listen(PORT, () => {
  console.log(`Blockchain Explorer API server running on port ${PORT}`);
  console.log(`Health check: http://localhost:${PORT}/api/health`);
});
