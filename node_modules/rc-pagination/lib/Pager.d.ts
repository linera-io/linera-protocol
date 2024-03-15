import React from 'react';
import type { PaginationProps } from './interface';
export interface PagerProps extends Pick<PaginationProps, 'itemRender'> {
    rootPrefixCls: string;
    page: number;
    active?: boolean;
    className?: string;
    showTitle: boolean;
    onClick?: (page: number) => void;
    onKeyPress?: (e: React.KeyboardEvent<HTMLLIElement>, onClick: PagerProps['onClick'], page: PagerProps['page']) => void;
}
declare const Pager: React.FC<PagerProps>;
export default Pager;
