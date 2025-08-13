# Linera Blockchain Explorer

A simple, modern blockchain explorer for the Linera protocol built with TypeScript and React.

## Features

- **Block Explorer**: View latest blocks with detailed information
- **Block Details**: Inspect individual blocks, including incoming bundles and messages
- **Chain View**: Browse different chains and their statistics
- **Search**: Find blocks by hash
- **Responsive Design**: Works on desktop and mobile devices

## Technology Stack

- **Frontend**: React 18 with TypeScript
- **Styling**: Tailwind CSS
- **Routing**: React Router
- **Database**: SQLite with better-sqlite3
- **Build Tool**: Vite
- **Icons**: Lucide React

## Prerequisites

- Node.js 16 or higher
- npm or yarn
- Access to a Linera indexer SQLite database

## Installation

1. Clone or navigate to the project directory:
   ```bash
   cd linera-explorer-new
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Set up environment variables (optional):
   ```bash
   cp .env.example .env
   # Edit .env to set VITE_DB_PATH to your database location
   ```

## Configuration

The explorer uses a client-server architecture:
- **Frontend**: React app running on port 3001
- **Backend**: Express API server running on port 3002

The backend connects to a SQLite database created by the Linera indexer. By default, it looks for the database at `../docker/indexer-data/indexer.db`.

You can configure the database path by setting the `DB_PATH` environment variable:

```bash
export DB_PATH="/path/to/your/indexer.db"
```

## Development

### Option 1: Start both frontend and backend together
```bash
npm run start
```

### Option 2: Start them separately
```bash
# Terminal 1 - Start the API server
npm run server

# Terminal 2 - Start the frontend
npm run dev
```

The explorer will be available at:
- Frontend: `http://localhost:3001`
- API: `http://localhost:3002`

## Building for Production

Build the application:

```bash
npm run build
```

The built files will be in the `dist` directory.

## Database Schema

The explorer expects the following SQLite tables:

### blocks
- `hash` (TEXT, PRIMARY KEY)
- `chain_id` (TEXT)
- `height` (INTEGER)
- `data` (BLOB)
- `created_at` (DATETIME)

### incoming_bundles
- `id` (INTEGER, PRIMARY KEY)
- `block_hash` (TEXT)
- `bundle_index` (INTEGER)
- `origin_chain_id` (TEXT)
- `action` (TEXT)
- `source_height` (INTEGER)
- `source_timestamp` (INTEGER)
- `source_cert_hash` (TEXT)
- `transaction_index` (INTEGER)
- `created_at` (DATETIME)

### posted_messages
- `id` (INTEGER, PRIMARY KEY)
- `bundle_id` (INTEGER)
- `message_index` (INTEGER)
- `authenticated_signer` (BLOB)
- `grant_amount` (INTEGER)
- `refund_grant_to` (BLOB)
- `message_kind` (TEXT)
- `message_data` (BLOB)
- `created_at` (DATETIME)

### blobs
- `hash` (TEXT, PRIMARY KEY)
- `type` (TEXT)
- `data` (BLOB)
- `created_at` (DATETIME)

## Usage

### Viewing Blocks
- Navigate to the home page to see the latest blocks
- Click on any block to view its details
- Use the search bar to find specific blocks by hash

### Exploring Chains
- Click "Chains" in the navigation to see all chains
- Each chain shows block count and latest height
- Click on a chain to see its blocks

### Block Details
- View complete block information including hash, height, and size
- See incoming bundles and their actions
- Inspect raw block data in hexadecimal format

## API Endpoints

The backend provides the following REST API endpoints:

- `GET /api/health` - Health check
- `GET /api/blocks` - Get latest blocks (with pagination)
- `GET /api/blocks/:hash` - Get specific block by hash
- `GET /api/blocks/:hash/bundles` - Get incoming bundles for a block
- `GET /api/bundles/:id/messages` - Get posted messages for a bundle
- `GET /api/chains` - Get all chains with statistics
- `GET /api/chains/:chainId/blocks` - Get blocks for a specific chain
- `GET /api/search?q=<query>` - Search blocks by hash prefix
- `GET /api/stats` - Get database statistics

## Production Notes

For production deployment, consider:

1. Add authentication and rate limiting to the API
2. Implement caching for better performance
3. Use environment variables for configuration
4. Add request logging and monitoring
5. Use a process manager like PM2 for the backend

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project follows the same license as the Linera protocol (Apache-2.0).

## Support

For issues related to the Linera protocol itself, please refer to the main Linera repository.
For explorer-specific issues, please create an issue in this repository.