import * as React from 'react';
export interface DotProps {
    prefixCls: string;
    value: number;
    style?: React.CSSProperties | ((dotValue: number) => React.CSSProperties);
    activeStyle?: React.CSSProperties | ((dotValue: number) => React.CSSProperties);
}
export default function Dot(props: DotProps): React.JSX.Element;
