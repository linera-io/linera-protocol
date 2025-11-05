/**
 * Application-wide constants and configuration
 */

// Pagination
export const BLOCKS_PER_PAGE = 50;
export const CHAINS_PER_PAGE = 50;
export const MAX_VISIBLE_PAGES = 7;

// Polling intervals (milliseconds)
export const REFRESH_INTERVAL = 5000;
export const RELATIVE_TIME_UPDATE_INTERVAL = 1000;

// Data display limits
export const BINARY_DATA_PREVIEW_BYTES = 100;
export const BINARY_DATA_MAX_DISPLAY_BYTES = 400;

// Hash formatting
export const HASH_FORMAT_SHORT = 'short' as const;
export const HASH_FORMAT_MEDIUM = 'medium' as const;
export const HASH_FORMAT_FULL = 'full' as const;

// API Configuration
export const API_BASE_URL = '/api';

// Feature flags (for future use)
export const FEATURES = {
  WEBSOCKET_ENABLED: false,
  SKELETON_LOADING: false,
} as const;
