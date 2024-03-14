import * as React from 'react';
import type { CustomizeComponent } from '../interface';
export interface ExpandedRowProps {
    prefixCls: string;
    component: CustomizeComponent;
    cellComponent: CustomizeComponent;
    className: string;
    expanded: boolean;
    children: React.ReactNode;
    colSpan: number;
    isEmpty: boolean;
}
declare function ExpandedRow(props: ExpandedRowProps): React.JSX.Element;
export default ExpandedRow;
