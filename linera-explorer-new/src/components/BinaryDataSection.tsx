import React, { useState } from 'react';
import { Copy, Check } from 'lucide-react';

interface BinaryDataSectionProps {
  data: Uint8Array;
  title: string;
  maxDisplayBytes?: number;
  className?: string;
}

export const BinaryDataSection: React.FC<BinaryDataSectionProps> = ({
  data,
  title,
  maxDisplayBytes = 400,
  className = ''
}) => {
  const [copied, setCopied] = useState(false);

  const formatBytes = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const copyToClipboard = async () => {
    try {
      const hexString = Array.from(data).map(byte => byte.toString(16).padStart(2, '0')).join('');
      await navigator.clipboard.writeText(hexString);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy binary data: ', err);
    }
  };

  const displayData = data.slice(0, maxDisplayBytes);
  const hexString = Array.from(displayData)
    .map(byte => byte.toString(16).padStart(2, '0'))
    .join(' ')
    .replace(/(.{48})/g, '$1\n'); // Add line breaks every 48 characters (16 bytes)

  return (
    <div className={`mt-3 pt-3 border-t border-linera-border/30 ${className}`}>
      <span className="text-sm text-linera-gray-light block mb-2">{title}</span>
      <div className="bg-linera-darker/50 rounded-lg border border-linera-border/50 p-4 font-mono text-xs overflow-x-auto">
        <div className="text-linera-gray-light mb-4 flex items-center justify-between">
          <div>
            <span>Size: {formatBytes(data.length)}</span>
            <span className="mx-2">•</span>
            <span>Type: Binary Data</span>
          </div>
          <button
            onClick={copyToClipboard}
            className="text-linera-gray-medium hover:text-linera-red transition-colors"
            title={copied ? 'Copied!' : 'Copy to clipboard'}
          >
            {copied ? (
              <Check className="w-4 h-4 text-green-400" />
            ) : (
              <Copy className="w-4 h-4" />
            )}
          </button>
        </div>
        <div className="text-linera-gray-light leading-relaxed max-h-32 overflow-y-auto">
          {hexString}
          {data.length > maxDisplayBytes && (
            <div className="text-linera-gray-medium mt-4">
              ... and {data.length - maxDisplayBytes} more bytes
            </div>
          )}
        </div>
      </div>
    </div>
  );
};