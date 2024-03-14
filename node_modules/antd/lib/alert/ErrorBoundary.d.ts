import * as React from 'react';
interface ErrorBoundaryProps {
    message?: React.ReactNode;
    description?: React.ReactNode;
    children?: React.ReactNode;
}
interface ErrorBoundaryStates {
    error?: Error | null;
    info?: {
        componentStack?: string;
    };
}
declare class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryStates> {
    state: {
        error: undefined;
        info: {
            componentStack: string;
        };
    };
    componentDidCatch(error: Error | null, info: object): void;
    render(): string | number | boolean | Iterable<React.ReactNode> | React.JSX.Element | null | undefined;
}
export default ErrorBoundary;
