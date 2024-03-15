import * as React from 'react';
import { Collection } from './Collection';
import { _rs } from './utils/observerUtil';
export { 
/** @private Test only for mock trigger resize event */
_rs, };
export interface SizeInfo {
    width: number;
    height: number;
    offsetWidth: number;
    offsetHeight: number;
}
export type OnResize = (size: SizeInfo, element: HTMLElement) => void;
export interface ResizeObserverProps {
    /** Pass to ResizeObserver.Collection with additional data */
    data?: any;
    children: React.ReactNode | ((ref: React.RefObject<any>) => React.ReactElement);
    disabled?: boolean;
    /** Trigger if element resized. Will always trigger when first time render. */
    onResize?: OnResize;
}
declare const RefResizeObserver: React.ForwardRefExoticComponent<ResizeObserverProps & React.RefAttributes<any>> & {
    Collection: typeof Collection;
};
export default RefResizeObserver;
