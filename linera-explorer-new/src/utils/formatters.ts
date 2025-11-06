/**
 * Centralized formatting utilities for the application
 */

export type HashFormat = 'short' | 'medium' | 'full';

/**
 * Format a hash string with configurable truncation
 * @param hash - The hash string to format
 * @param format - The format type (short: 8+8, medium: 12+12, full: no truncation)
 * @returns Formatted hash string
 */
export const formatHash = (hash: string, format: HashFormat = 'short'): string => {
  if (!hash) return '';
  if (format === 'full') return hash;

  const lengths: Record<Exclude<HashFormat, 'full'>, [number, number]> = {
    short: [8, 8],
    medium: [12, 12],
  };

  const [start, end] = lengths[format];
  return `${hash.slice(0, start)}...${hash.slice(-end)}`;
};

/**
 * Format bytes to human-readable format (B, KB, MB)
 * @param bytes - Number of bytes
 * @returns Formatted string with unit
 */
export const formatBytes = (bytes: number): string => {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
};

/**
 * Format timestamp to relative time (e.g., "5s ago", "3m ago", "2h ago")
 * @param timestamp - ISO timestamp string or timestamp in microseconds
 * @returns Formatted relative time string
 */
export const formatTimestamp = (timestamp: string | number): string => {
  // Handle microsecond timestamps (convert to Date)
  const date = typeof timestamp === 'number'
    ? new Date(timestamp / 1000)
    : new Date(timestamp.includes('Z') || timestamp.includes('+') ? timestamp : timestamp + 'Z');

  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);
  const diffMin = Math.floor(diffSec / 60);
  const diffHour = Math.floor(diffMin / 60);

  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffMin < 60) return `${diffMin}m ago`;
  if (diffHour < 24) return `${diffHour}h ago`;
  return date.toLocaleDateString();
};

/**
 * Calculate relative time between two dates
 * @param date - The date to compare
 * @param now - Current date/time
 * @returns Human-readable relative time
 */
export const formatRelativeTime = (date: Date, now: Date): string => {
  const diffMs = now.getTime() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);

  if (diffSec < 60) return `${diffSec} seconds ago`;
  const diffMin = Math.floor(diffSec / 60);
  return `${diffMin} minutes ago`;
};

/**
 * Format chain ID with configurable truncation
 * @param chainId - The chain ID to format
 * @param length - Number of characters to show from start (default: 64)
 * @returns Formatted chain ID
 */
export const formatChainId = (chainId: string, length: number = 64): string => {
  if (!chainId || chainId.length <= length) return chainId;
  return `${chainId.slice(0, length)}...`;
};
