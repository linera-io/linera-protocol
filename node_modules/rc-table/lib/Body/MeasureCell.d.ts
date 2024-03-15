import * as React from 'react';
export interface MeasureCellProps {
    columnKey: React.Key;
    onColumnResize: (key: React.Key, width: number) => void;
}
export default function MeasureCell({ columnKey, onColumnResize }: MeasureCellProps): React.JSX.Element;
