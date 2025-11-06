import React from 'react';

interface LoadingSpinnerProps {
  size?: 'sm' | 'md' | 'lg';
  className?: string;
  message?: string;
}

/**
 * Reusable loading spinner with optional message
 */
export const LoadingSpinner: React.FC<LoadingSpinnerProps> = ({
  size = 'md',
  className = '',
  message,
}) => {
  const sizeClasses = {
    sm: 'h-4 w-4',
    md: 'h-8 w-8',
    lg: 'h-12 w-12',
  };

  return (
    <div className={`flex flex-col justify-center items-center h-64 ${className}`}>
      <div className={`loading-spinner ${sizeClasses[size]}`}></div>
      {message && (
        <p className="mt-4 text-linera-gray-light text-sm">{message}</p>
      )}
    </div>
  );
};
