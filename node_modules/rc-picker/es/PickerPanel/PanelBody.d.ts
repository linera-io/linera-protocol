import * as React from 'react';
import type { DisabledDate } from '../interface';
export interface PanelBodyProps<DateType = any> {
    rowNum: number;
    colNum: number;
    baseDate: DateType;
    titleFormat?: string;
    getCellDate: (date: DateType, offset: number) => DateType;
    getCellText: (date: DateType) => React.ReactNode;
    getCellClassName: (date: DateType) => Record<string, any>;
    disabledDate?: DisabledDate<DateType>;
    headerCells?: React.ReactNode[];
    prefixColumn?: (date: DateType) => React.ReactNode;
    rowClassName?: (date: DateType) => string;
    cellSelection?: boolean;
}
export default function PanelBody<DateType extends object = any>(props: PanelBodyProps<DateType>): React.JSX.Element;
