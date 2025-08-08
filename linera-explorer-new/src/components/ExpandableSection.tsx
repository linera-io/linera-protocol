import React, { useState } from 'react';
import { ChevronDown, ChevronRight } from 'lucide-react';

interface ExpandableSectionProps {
  title: string;
  count: number;
  icon: React.ReactNode;
  children: React.ReactNode;
  defaultExpanded?: boolean;
  emptyMessage?: string;
}

export const ExpandableSection: React.FC<ExpandableSectionProps> = ({
  title,
  count,
  icon,
  children,
  defaultExpanded = false,
  emptyMessage = 'No data found'
}) => {
  const [isExpanded, setIsExpanded] = useState(defaultExpanded);

  if (count === 0) {
    return null;
  }

  return (
    <div className="card">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between p-0 bg-transparent border-none cursor-pointer group"
      >
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-linera-red/20 rounded-lg border border-linera-red/30 group-hover:bg-linera-red/30 transition-colors">
            {icon}
          </div>
          <div className="text-left">
            <h2 className="text-xl font-epilogue font-semibold text-white group-hover:text-linera-red transition-colors">
              {title}
            </h2>
            <p className="text-sm text-linera-gray-light">
              {count} item{count !== 1 ? 's' : ''} found
            </p>
          </div>
        </div>
        <div className="flex items-center space-x-2 text-linera-gray-medium group-hover:text-linera-red transition-colors">
          <span className="text-sm">{isExpanded ? 'Hide' : 'Show'} Details</span>
          {isExpanded ? (
            <ChevronDown className="w-5 h-5" />
          ) : (
            <ChevronRight className="w-5 h-5" />
          )}
        </div>
      </button>

      {isExpanded && (
        <div className="mt-6 border-t border-linera-border/30 pt-6 animate-slide-up">
          {count > 0 ? children : (
            <p className="text-linera-gray-light text-center py-8">{emptyMessage}</p>
          )}
        </div>
      )}
    </div>
  );
};