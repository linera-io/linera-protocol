import * as React from 'react';
import type { PanelMode, SharedPanelProps } from '../../interface';
export interface DatePanelProps<DateType extends object> extends SharedPanelProps<DateType> {
    panelName?: PanelMode;
    rowClassName?: (date: DateType) => string;
    /** Used for `WeekPanel` */
    mode?: PanelMode;
    cellSelection?: boolean;
}
export default function DatePanel<DateType extends object = any>(props: DatePanelProps<DateType>): React.JSX.Element;
