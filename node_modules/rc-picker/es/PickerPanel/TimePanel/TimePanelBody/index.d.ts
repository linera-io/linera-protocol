import * as React from 'react';
import type { SharedPanelProps, SharedTimeProps } from '../../../interface';
export type TimePanelBodyProps<DateType extends object = any> = SharedPanelProps<DateType>;
export default function TimePanelBody<DateType extends object = any>(props: SharedTimeProps<DateType>): React.JSX.Element;
