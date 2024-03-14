import React from 'react';
import throttleByAnimationFrame from '../_util/throttleByAnimationFrame';
export interface AffixProps {
    /** Triggered when the specified offset is reached from the top of the window */
    offsetTop?: number;
    /** Triggered when the specified offset is reached from the bottom of the window */
    offsetBottom?: number;
    style?: React.CSSProperties;
    /** Callback function triggered when fixed state changes */
    onChange?: (affixed?: boolean) => void;
    /** Set the element that Affix needs to listen to its scroll event, the value is a function that returns the corresponding DOM element */
    target?: () => Window | HTMLElement | null;
    prefixCls?: string;
    className?: string;
    rootClassName?: string;
    children: React.ReactNode;
}
interface AffixRef {
    updatePosition: ReturnType<typeof throttleByAnimationFrame>;
}
declare const Affix: React.ForwardRefExoticComponent<AffixProps & React.RefAttributes<AffixRef>>;
export default Affix;
