import * as React from 'react';
import type { HeaderProps } from '../Header/Header';
import type { Direction } from '../interface';
export interface FixedHeaderProps<RecordType> extends HeaderProps<RecordType> {
    className: string;
    noData: boolean;
    maxContentScroll: boolean;
    colWidths: readonly number[];
    columCount: number;
    direction: Direction;
    fixHeader: boolean;
    stickyTopOffset?: number;
    stickyBottomOffset?: number;
    stickyClassName?: string;
    onScroll: (info: {
        currentTarget: HTMLDivElement;
        scrollLeft?: number;
    }) => void;
    children: (info: HeaderProps<RecordType>) => React.ReactNode;
}
/** Return a table in div as fixed element which contains sticky info */
declare const _default: React.MemoExoticComponent<React.ForwardRefExoticComponent<FixedHeaderProps<unknown> & React.RefAttributes<HTMLDivElement>>>;
export default _default;
