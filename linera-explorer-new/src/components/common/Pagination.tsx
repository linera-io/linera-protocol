import React from 'react';

interface PaginationProps {
  currentPage: number;
  totalPages: number;
  onPageChange: (page: number) => void;
  maxVisiblePages?: number;
}

/**
 * Reusable pagination component with smart page number display
 */
export const Pagination: React.FC<PaginationProps> = ({
  currentPage,
  totalPages,
  onPageChange,
  maxVisiblePages = 7,
}) => {
  if (totalPages <= 1) return null;

  // Calculate which page numbers to display
  const getVisiblePages = (): number[] => {
    const pages: number[] = [];
    const visibleCount = Math.min(totalPages, maxVisiblePages);

    for (let i = 0; i < visibleCount; i++) {
      let pageNum: number;

      if (totalPages <= maxVisiblePages) {
        pageNum = i + 1;
      } else if (currentPage <= Math.floor(maxVisiblePages / 2) + 1) {
        pageNum = i + 1;
      } else if (currentPage >= totalPages - Math.floor(maxVisiblePages / 2)) {
        pageNum = totalPages - (maxVisiblePages - 1) + i;
      } else {
        pageNum = currentPage - Math.floor(maxVisiblePages / 2) + i;
      }

      pages.push(pageNum);
    }

    return pages;
  };

  const visiblePages = getVisiblePages();

  return (
    <div className="flex justify-center items-center space-x-2">
      {/* Previous Button */}
      <button
        onClick={() => onPageChange(Math.max(1, currentPage - 1))}
        disabled={currentPage === 1}
        className="px-4 py-2 bg-linera-darker border border-linera-border rounded-lg text-white disabled:opacity-50 disabled:cursor-not-allowed hover:bg-linera-red hover:border-linera-red transition-colors"
      >
        Previous
      </button>

      {/* Page Numbers */}
      <div className="flex space-x-2">
        {visiblePages.map((pageNum) => (
          <button
            key={pageNum}
            onClick={() => onPageChange(pageNum)}
            className={`px-4 py-2 rounded-lg transition-colors ${
              currentPage === pageNum
                ? 'bg-linera-red text-white border border-linera-red'
                : 'bg-linera-darker border border-linera-border text-white hover:bg-linera-red hover:border-linera-red'
            }`}
          >
            {pageNum}
          </button>
        ))}
      </div>

      {/* Next Button */}
      <button
        onClick={() => onPageChange(Math.min(totalPages, currentPage + 1))}
        disabled={currentPage === totalPages}
        className="px-4 py-2 bg-linera-darker border border-linera-border rounded-lg text-white disabled:opacity-50 disabled:cursor-not-allowed hover:bg-linera-red hover:border-linera-red transition-colors"
      >
        Next
      </button>
    </div>
  );
};
