import * as React from 'react';
export type MaskProps = {
    prefixCls: string;
    visible: boolean;
    motionName?: string;
    style?: React.CSSProperties;
    maskProps?: React.HTMLAttributes<HTMLDivElement>;
    className?: string;
};
export default function Mask(props: MaskProps): React.JSX.Element;
