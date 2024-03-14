import * as React from 'react';
export type InnerProps = Pick<React.HTMLAttributes<HTMLDivElement>, 'role' | 'id'>;
interface FillerProps {
    prefixCls?: string;
    /** Virtual filler height. Should be `count * itemMinHeight` */
    height: number;
    /** Set offset of visible items. Should be the top of start item position */
    offsetY?: number;
    offsetX?: number;
    scrollWidth?: number;
    children: React.ReactNode;
    onInnerResize?: () => void;
    innerProps?: InnerProps;
    rtl: boolean;
    extra?: React.ReactNode;
}
/**
 * Fill component to provided the scroll content real height.
 */
declare const Filler: React.ForwardRefExoticComponent<FillerProps & React.RefAttributes<HTMLDivElement>>;
export default Filler;
