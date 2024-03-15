import * as React from 'react';
import type { ResizeObserverProps } from '..';
export interface SingleObserverProps extends ResizeObserverProps {
    children: React.ReactElement | ((ref: React.RefObject<Element>) => React.ReactElement);
}
declare const RefSingleObserver: React.ForwardRefExoticComponent<SingleObserverProps & React.RefAttributes<HTMLElement>>;
export default RefSingleObserver;
