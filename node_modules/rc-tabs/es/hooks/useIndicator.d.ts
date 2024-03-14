import React from 'react';
import type { TabOffset } from '../interface';
export type GetIndicatorSize = number | ((origin: number) => number);
interface UseIndicatorOptions {
    activeTabOffset: TabOffset;
    horizontal: boolean;
    rtl: boolean;
    indicator?: {
        size?: GetIndicatorSize;
        align?: 'start' | 'center' | 'end';
    };
}
declare const useIndicator: (options: UseIndicatorOptions) => {
    style: React.CSSProperties;
};
export default useIndicator;
