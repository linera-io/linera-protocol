import * as React from 'react';
export interface MarkObj {
    style?: React.CSSProperties;
    label?: React.ReactNode;
}
export interface InternalMarkObj extends MarkObj {
    value: number;
}
export interface MarksProps {
    prefixCls: string;
    marks?: InternalMarkObj[];
    onClick: (value: number) => void;
}
export default function Marks(props: MarksProps): React.JSX.Element;
