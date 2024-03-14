import * as React from 'react';
import type { SharedPanelProps } from '../../interface';
export type TimePanelProps<DateType extends object> = SharedPanelProps<DateType>;
export default function TimePanel<DateType extends object = any>(props: TimePanelProps<DateType>): React.JSX.Element;
