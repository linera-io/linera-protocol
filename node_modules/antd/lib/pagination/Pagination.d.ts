import type { PaginationLocale, PaginationProps as RcPaginationProps } from 'rc-pagination';
import * as React from 'react';
export interface PaginationProps extends RcPaginationProps {
    showQuickJumper?: boolean | {
        goButton?: React.ReactNode;
    };
    size?: 'default' | 'small';
    responsive?: boolean;
    role?: string;
    totalBoundaryShowSizeChanger?: number;
    rootClassName?: string;
}
export type PaginationPosition = 'top' | 'bottom' | 'both';
export type PaginationAlign = 'start' | 'center' | 'end';
export interface PaginationConfig extends Omit<PaginationProps, 'rootClassName'> {
    position?: PaginationPosition;
    align?: PaginationAlign;
}
export type { PaginationLocale };
declare const Pagination: React.FC<PaginationProps>;
export default Pagination;
