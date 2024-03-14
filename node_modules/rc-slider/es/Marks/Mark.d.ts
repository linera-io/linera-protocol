import * as React from 'react';
export interface MarkProps {
    prefixCls: string;
    children?: React.ReactNode;
    style?: React.CSSProperties;
    value: number;
    onClick: (value: number) => void;
}
export default function Mark(props: MarkProps): React.JSX.Element;
