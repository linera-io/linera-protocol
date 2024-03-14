import * as React from 'react';
export interface MeasureCellProps {
    prefixCls: string;
    onColumnResize: (key: React.Key, width: number) => void;
    columnsKey: React.Key[];
}
export default function MeasureRow({ prefixCls, columnsKey, onColumnResize }: MeasureCellProps): React.JSX.Element;
