import * as React from 'react';
export interface RefContextProps {
    panel?: React.Ref<HTMLDivElement>;
}
export declare const RefContext: React.Context<RefContextProps>;
