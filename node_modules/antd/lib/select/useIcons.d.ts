import type { ReactNode } from 'react';
import * as React from 'react';
type RenderNode = React.ReactNode | ((props: any) => React.ReactNode);
export default function useIcons({ suffixIcon, clearIcon, menuItemSelectedIcon, removeIcon, loading, multiple, hasFeedback, prefixCls, showSuffixIcon, feedbackIcon, showArrow, componentName, }: {
    suffixIcon?: React.ReactNode;
    clearIcon?: RenderNode;
    menuItemSelectedIcon?: RenderNode;
    removeIcon?: RenderNode;
    loading?: boolean;
    multiple?: boolean;
    hasFeedback?: boolean;
    feedbackIcon?: ReactNode;
    prefixCls: string;
    showSuffixIcon?: boolean;
    showArrow?: boolean;
    componentName: string;
}): {
    clearIcon: string | number | boolean | Iterable<ReactNode> | React.JSX.Element | ((props: any) => ReactNode);
    suffixIcon: React.JSX.Element | (({ open, showSearch }: {
        open: boolean;
        showSearch: boolean;
    }) => React.JSX.Element | null) | null;
    itemIcon: string | number | boolean | Iterable<ReactNode> | React.JSX.Element | ((props: any) => ReactNode) | null;
    removeIcon: string | number | boolean | Iterable<ReactNode> | React.JSX.Element | ((props: any) => ReactNode) | null;
};
export {};
