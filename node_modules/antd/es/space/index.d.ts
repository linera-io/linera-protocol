import * as React from 'react';
import type { SizeType } from '../config-provider/SizeContext';
import Compact from './Compact';
export { SpaceContext } from './context';
export type SpaceSize = SizeType | number;
export interface SpaceProps extends React.HTMLAttributes<HTMLDivElement> {
    prefixCls?: string;
    className?: string;
    rootClassName?: string;
    style?: React.CSSProperties;
    size?: SpaceSize | [SpaceSize, SpaceSize];
    direction?: 'horizontal' | 'vertical';
    align?: 'start' | 'end' | 'center' | 'baseline';
    split?: React.ReactNode;
    wrap?: boolean;
    classNames?: {
        item: string;
    };
    styles?: {
        item: React.CSSProperties;
    };
}
type CompoundedComponent = React.ForwardRefExoticComponent<SpaceProps & React.RefAttributes<HTMLDivElement>> & {
    Compact: typeof Compact;
};
declare const CompoundedSpace: CompoundedComponent;
export default CompoundedSpace;
