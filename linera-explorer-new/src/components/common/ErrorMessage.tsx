import React from 'react';
import { AlertCircle } from 'lucide-react';

interface ErrorMessageProps {
  title?: string;
  message: string;
  icon?: React.ReactNode;
  className?: string;
}

/**
 * Reusable error message component with consistent styling
 */
export const ErrorMessage: React.FC<ErrorMessageProps> = ({
  title = 'Error',
  message,
  icon,
  className = '',
}) => {
  return (
    <div className={`card bg-red-500/10 border-red-500/30 ${className}`}>
      <div className="text-red-300 flex items-start space-x-3">
        <div className="flex-shrink-0">
          {icon || <AlertCircle className="w-5 h-5" />}
        </div>
        <div>
          {title && <div className="font-semibold mb-1">{title}</div>}
          <div>{message}</div>
        </div>
      </div>
    </div>
  );
};
