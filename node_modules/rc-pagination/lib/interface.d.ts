import type React from 'react';
export interface PaginationLocale {
    items_per_page?: string;
    jump_to?: string;
    jump_to_confirm?: string;
    page?: string;
    prev_page?: string;
    next_page?: string;
    prev_5?: string;
    next_5?: string;
    prev_3?: string;
    next_3?: string;
    page_size?: string;
}
export interface PaginationData {
    className: string;
    selectPrefixCls: string;
    prefixCls: string;
    pageSizeOptions: string[] | number[];
    current: number;
    defaultCurrent: number;
    total: number;
    totalBoundaryShowSizeChanger?: number;
    pageSize: number;
    defaultPageSize: number;
    hideOnSinglePage: boolean;
    showSizeChanger: boolean;
    showLessItems: boolean;
    showPrevNextJumpers: boolean;
    showQuickJumper: boolean | object;
    showTitle: boolean;
    simple: boolean;
    disabled: boolean;
    locale: PaginationLocale;
    style: React.CSSProperties;
    selectComponentClass: React.ComponentType;
    prevIcon: React.ComponentType | React.ReactNode;
    nextIcon: React.ComponentType | React.ReactNode;
    jumpPrevIcon: React.ComponentType | React.ReactNode;
    jumpNextIcon: React.ComponentType | React.ReactNode;
}
export interface PaginationProps extends Partial<PaginationData>, React.AriaAttributes {
    onChange?: (page: number, pageSize: number) => void;
    onShowSizeChange?: (current: number, size: number) => void;
    itemRender?: (page: number, type: 'page' | 'prev' | 'next' | 'jump-prev' | 'jump-next', element: React.ReactNode) => React.ReactNode;
    showTotal?: (total: number, range: [number, number]) => React.ReactNode;
    role?: React.AriaRole | undefined;
}
export interface PaginationState {
    current: number;
    currentInputValue: number;
    pageSize: number;
}
