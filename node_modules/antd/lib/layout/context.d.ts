import * as React from 'react';
export interface LayoutContextProps {
    siderHook: {
        addSider: (id: string) => void;
        removeSider: (id: string) => void;
    };
}
export declare const LayoutContext: React.Context<LayoutContextProps>;
