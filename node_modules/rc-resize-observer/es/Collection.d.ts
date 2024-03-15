import * as React from 'react';
import type { SizeInfo } from '.';
type onCollectionResize = (size: SizeInfo, element: HTMLElement, data: any) => void;
export declare const CollectionContext: React.Context<onCollectionResize>;
export interface ResizeInfo {
    size: SizeInfo;
    data: any;
    element: HTMLElement;
}
export interface CollectionProps {
    /** Trigger when some children ResizeObserver changed. Collect by frame render level */
    onBatchResize?: (resizeInfo: ResizeInfo[]) => void;
    children?: React.ReactNode;
}
/**
 * Collect all the resize event from children ResizeObserver
 */
export declare function Collection({ children, onBatchResize }: CollectionProps): React.JSX.Element;
export {};
