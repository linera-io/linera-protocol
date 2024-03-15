import React from 'react';
export interface SpaceContextType {
    latestIndex: number;
}
export declare const SpaceContext: React.Context<SpaceContextType>;
export declare const SpaceContextProvider: React.Provider<SpaceContextType>;
