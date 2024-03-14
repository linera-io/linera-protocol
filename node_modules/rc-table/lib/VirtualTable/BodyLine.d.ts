import * as React from 'react';
import type { FlattenData } from '../hooks/useFlattenRecords';
export interface BodyLineProps<RecordType = any> {
    data: FlattenData<RecordType>;
    index: number;
    className?: string;
    style?: React.CSSProperties;
    rowKey: React.Key;
    /** Render cell only when it has `rowSpan > 1` */
    extra?: boolean;
    getHeight?: (rowSpan: number) => number;
}
declare const ResponseBodyLine: React.ForwardRefExoticComponent<BodyLineProps<any> & React.RefAttributes<HTMLDivElement>>;
export default ResponseBodyLine;
