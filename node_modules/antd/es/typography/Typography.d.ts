import * as React from 'react';
import type { DirectionType } from '../config-provider';
export interface TypographyProps<C extends keyof JSX.IntrinsicElements> extends React.HTMLAttributes<HTMLElement> {
    id?: string;
    prefixCls?: string;
    className?: string;
    rootClassName?: string;
    style?: React.CSSProperties;
    children?: React.ReactNode;
    ['aria-label']?: string;
    direction?: DirectionType;
}
interface InternalTypographyProps<C extends keyof JSX.IntrinsicElements> extends TypographyProps<C> {
    /** @deprecated Use `ref` directly if using React 16 */
    setContentRef?: (node: HTMLElement) => void;
}
declare const Typography: React.ForwardRefExoticComponent<InternalTypographyProps<keyof JSX.IntrinsicElements> & React.RefAttributes<HTMLElement>>;
export default Typography;
