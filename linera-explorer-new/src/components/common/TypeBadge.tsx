import React from 'react';

export type BadgeVariant = 'system' | 'user' | 'accept' | 'reject' | 'oracle';

interface TypeBadgeProps {
  type: string;
  variant?: BadgeVariant;
  className?: string;
}

/**
 * Reusable badge component for displaying types with consistent styling
 */
export const TypeBadge: React.FC<TypeBadgeProps> = ({
  type,
  variant,
  className = ''
}) => {
  // Auto-detect variant if not provided
  const detectedVariant = variant || detectVariant(type);

  const variantStyles: Record<BadgeVariant, string> = {
    system: 'bg-blue-500/20 text-blue-400 border border-blue-500/30',
    user: 'bg-purple-500/20 text-purple-400 border border-purple-500/30',
    accept: 'bg-green-500/20 text-green-400 border border-green-500/30',
    reject: 'bg-red-500/20 text-red-400 border border-red-500/30',
    oracle: 'bg-green-500/20 text-green-400 border border-green-500/30',
  };

  return (
    <span className={`px-2 py-1 rounded text-xs font-medium ${variantStyles[detectedVariant]} ${className}`}>
      {type}
    </span>
  );
};

// Helper function to auto-detect variant from type string
function detectVariant(type: string): BadgeVariant {
  const typeLower = type.toLowerCase();
  if (typeLower === 'system') return 'system';
  if (typeLower === 'user') return 'user';
  if (typeLower === 'accept') return 'accept';
  if (typeLower === 'reject') return 'reject';
  return 'user'; // default
}
