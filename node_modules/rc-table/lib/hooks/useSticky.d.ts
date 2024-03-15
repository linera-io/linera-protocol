import type { TableSticky } from '../interface';
/** Sticky header hooks */
export default function useSticky(sticky: boolean | TableSticky, prefixCls: string): {
    isSticky: boolean;
    offsetHeader: number;
    offsetSummary: number;
    offsetScroll: number;
    stickyClassName: string;
    container: Window | HTMLElement;
};
