import * as React from 'react';
import type { Breakpoint } from '../_util/responsiveObserver';
import DescriptionsContext from './DescriptionsContext';
import type { DescriptionsItemProps } from './Item';
import DescriptionsItem from './Item';
interface CompoundedComponent {
    Item: typeof DescriptionsItem;
}
export interface InternalDescriptionsItemType extends DescriptionsItemProps {
    key?: React.Key;
}
export interface DescriptionsItemType extends Omit<InternalDescriptionsItemType, 'span'> {
    span?: number | {
        [key in Breakpoint]?: number;
    };
}
export interface DescriptionsProps {
    prefixCls?: string;
    className?: string;
    rootClassName?: string;
    style?: React.CSSProperties;
    bordered?: boolean;
    size?: 'middle' | 'small' | 'default';
    /**
     * @deprecated use `items` instead
     */
    children?: React.ReactNode;
    title?: React.ReactNode;
    extra?: React.ReactNode;
    column?: number | Partial<Record<Breakpoint, number>>;
    layout?: 'horizontal' | 'vertical';
    colon?: boolean;
    labelStyle?: React.CSSProperties;
    contentStyle?: React.CSSProperties;
    items?: DescriptionsItemType[];
    id?: string;
}
declare const Descriptions: React.FC<DescriptionsProps> & CompoundedComponent;
export type { DescriptionsContextProps } from './DescriptionsContext';
export { DescriptionsContext };
export default Descriptions;
