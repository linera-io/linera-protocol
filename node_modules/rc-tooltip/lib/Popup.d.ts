import * as React from 'react';
export interface ContentProps {
    prefixCls?: string;
    children: (() => React.ReactNode) | React.ReactNode;
    id?: string;
    overlayInnerStyle?: React.CSSProperties;
    className?: string;
    style?: React.CSSProperties;
}
export default function Popup(props: ContentProps): React.JSX.Element;
