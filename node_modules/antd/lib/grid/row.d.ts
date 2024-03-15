import * as React from 'react';
import type { Breakpoint } from '../_util/responsiveObserver';
declare const RowAligns: readonly ["top", "middle", "bottom", "stretch"];
declare const RowJustify: readonly ["start", "end", "center", "space-around", "space-between", "space-evenly"];
type Responsive = 'xxl' | 'xl' | 'lg' | 'md' | 'sm' | 'xs';
type ResponsiveLike<T> = {
    [key in Responsive]?: T;
};
export type Gutter = number | undefined | Partial<Record<Breakpoint, number>>;
type ResponsiveAligns = ResponsiveLike<(typeof RowAligns)[number]>;
type ResponsiveJustify = ResponsiveLike<(typeof RowJustify)[number]>;
export interface RowProps extends React.HTMLAttributes<HTMLDivElement> {
    gutter?: Gutter | [Gutter, Gutter];
    align?: (typeof RowAligns)[number] | ResponsiveAligns;
    justify?: (typeof RowJustify)[number] | ResponsiveJustify;
    prefixCls?: string;
    wrap?: boolean;
}
declare const Row: React.ForwardRefExoticComponent<RowProps & React.RefAttributes<HTMLDivElement>>;
export default Row;
