import { HEX_64_PATTERN, HEX_64_MIN_LENGTH, HEX_64_ERROR_MESSAGE } from '../config/constants';

/**
 * Generic hook for client-side search with optional server fallback
 */

interface UseLocalSearchOptions<T> {
  /**
   * Array of items to search locally
   */
  items: T[];

  /**
   * Function to extract the searchable value from an item
   */
  getSearchValue: (item: T) => string;

  /**
   * Regex pattern to validate search query (optional, defaults to 64-char hex)
   */
  validationPattern?: RegExp;

  /**
   * Error message when validation fails (optional, defaults to hex string message)
   */
  validationError?: string;

  /**
   * Minimum query length (default: 64)
   */
  minLength?: number;
}

interface SearchResult<T> {
  found: boolean;
  item?: T;
  error?: string;
}

/**
 * Searches for an item in a local array with optional validation
 *
 * @param query - The search query string
 * @param options - Search configuration options
 * @returns SearchResult with found item or error
 */
export const useLocalSearch = <T>() => {
  const search = (
    query: string,
    options: UseLocalSearchOptions<T>
  ): SearchResult<T> => {
    const {
      items,
      getSearchValue,
      validationPattern = HEX_64_PATTERN,
      validationError = HEX_64_ERROR_MESSAGE,
      minLength = HEX_64_MIN_LENGTH
    } = options;

    // Check minimum length
    if (query.length < minLength) {
      return { found: false };
    }

    // Validate against pattern if provided
    if (validationPattern && !validationPattern.test(query)) {
      return { found: false, error: validationError };
    }

    // Search in local items (case-insensitive)
    const normalizedQuery = query.toLowerCase();
    const foundItem = items.find(item => {
      const value = getSearchValue(item).toLowerCase();
      return value === normalizedQuery || value.includes(normalizedQuery);
    });

    if (foundItem) {
      return { found: true, item: foundItem };
    }

    return { found: false };
  };

  return { search };
};
