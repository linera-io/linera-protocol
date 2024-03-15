import * as React from 'react';
export interface WatermarkContextProps {
    add: (ele: HTMLElement) => void;
    remove: (ele: HTMLElement) => void;
}
declare const WatermarkContext: React.Context<WatermarkContextProps>;
export declare function usePanelRef(panelSelector?: string): (ele: HTMLElement | null) => void;
export default WatermarkContext;
