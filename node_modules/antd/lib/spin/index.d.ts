import * as React from 'react';
declare const SpinSizes: readonly ["small", "default", "large"];
export type SpinSize = (typeof SpinSizes)[number];
export type SpinIndicator = React.ReactElement<HTMLElement>;
export interface SpinProps {
    prefixCls?: string;
    className?: string;
    rootClassName?: string;
    spinning?: boolean;
    style?: React.CSSProperties;
    size?: SpinSize;
    tip?: React.ReactNode;
    delay?: number;
    wrapperClassName?: string;
    indicator?: SpinIndicator;
    children?: React.ReactNode;
    fullscreen?: boolean;
}
export type SpinType = React.FC<SpinProps> & {
    setDefaultIndicator: (indicator: React.ReactNode) => void;
};
declare const Spin: SpinType;
export default Spin;
