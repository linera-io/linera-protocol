import * as React from 'react';
export interface PathRegisterContextProps {
    registerPath: (key: string, keyPath: string[]) => void;
    unregisterPath: (key: string, keyPath: string[]) => void;
}
export declare const PathRegisterContext: React.Context<PathRegisterContextProps>;
export declare function useMeasure(): PathRegisterContextProps;
export declare const PathTrackerContext: React.Context<string[]>;
export declare function useFullPath(eventKey?: string): string[];
export interface PathUserContextProps {
    isSubPathKey: (pathKeys: string[], eventKey: string) => boolean;
}
export declare const PathUserContext: React.Context<PathUserContextProps>;
