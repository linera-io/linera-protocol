import React, { useState } from 'react';
import { Copy, Check } from 'lucide-react';

interface CopyableHashProps {
  value: string;
  format?: 'short' | 'full';
  className?: string;
  showCopyIcon?: boolean;
}

export const CopyableHash: React.FC<CopyableHashProps> = ({
  value,
  format = 'short',
  className = '',
  showCopyIcon = true
}) => {
  const [copied, setCopied] = useState(false);

  const formatHash = (hash: string) => {
    if (format === 'short') {
      return `${hash.slice(0, 12)}...${hash.slice(-12)}`;
    }
    return hash;
  };

  const copyToClipboard = async (text: string, event?: React.MouseEvent) => {
    event?.stopPropagation();
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy text: ', err);
    }
  };

  return (
    <div className={`relative group ${className}`}>
      <div className={`font-mono text-sm bg-linera-darker/80 px-3 py-2 rounded border border-linera-border/50 text-linera-gray-light group-hover:text-white transition-colors cursor-pointer ${showCopyIcon ? 'pr-12' : ''}`}
           onClick={(e) => copyToClipboard(value, e)}>
        {formatHash(value)}
      </div>
      {showCopyIcon && (
        <button
          onClick={(e) => copyToClipboard(value, e)}
          className="absolute right-3 top-1/2 transform -translate-y-1/2 text-linera-gray-medium hover:text-linera-red transition-colors"
          title={copied ? 'Copied!' : 'Copy to clipboard'}
        >
          {copied ? (
            <Check className="w-4 h-4 text-green-400" />
          ) : (
            <Copy className="w-4 h-4" />
          )}
        </button>
      )}
    </div>
  );
};