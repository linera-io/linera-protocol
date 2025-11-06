import { useState, useEffect, useRef } from 'react';
import { BLOCKS_PER_PAGE, CHAINS_PER_PAGE } from '../config/constants';
import { BlockchainAPI } from '../utils/database';

interface UsePaginationOptions {
  itemsPerPage?: number;
  fetchTotalCount: () => Promise<number>;
}

export const usePagination = ({
  itemsPerPage = BLOCKS_PER_PAGE,
  fetchTotalCount
}: UsePaginationOptions) => {
  const [currentPage, setCurrentPage] = useState(1);
  const [totalItems, setTotalItems] = useState(0);
  const fetcherRef = useRef(fetchTotalCount);

  // Update ref when fetcher changes
  useEffect(() => {
    fetcherRef.current = fetchTotalCount;
  });

  const offset = (currentPage - 1) * itemsPerPage;
  const totalPages = Math.ceil(totalItems / itemsPerPage);

  // Fetch total count - runs once on mount and when the component needs a refresh
  useEffect(() => {
    const fetchCount = async () => {
      try {
        const count = await fetcherRef.current();
        setTotalItems(count);
      } catch (err) {
        console.error('Failed to fetch total count:', err);
      }
    };
    fetchCount();
  }, []);

  return {
    currentPage,
    setCurrentPage,
    totalItems,
    totalPages,
    offset,
    itemsPerPage
  };
};

// Convenience hook for blocks pagination
export const useBlocksPagination = (fetchTotalCount: () => Promise<number>) => {
  return usePagination({
    itemsPerPage: BLOCKS_PER_PAGE,
    fetchTotalCount
  });
};

// Convenience hook for chains pagination
export const useChainsPagination = () => {
  return usePagination({
    itemsPerPage: CHAINS_PER_PAGE,
    fetchTotalCount: async () => {
      const api = new BlockchainAPI();
      return api.getChainsCount();
    }
  });
};
