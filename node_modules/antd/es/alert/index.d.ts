import type React from 'react';
import type { AlertProps } from './Alert';
import ErrorBoundary from './ErrorBoundary';
export type { AlertProps } from './Alert';
type CompoundedComponent = React.FC<AlertProps> & {
    ErrorBoundary: typeof ErrorBoundary;
};
declare const Alert: CompoundedComponent;
export default Alert;
