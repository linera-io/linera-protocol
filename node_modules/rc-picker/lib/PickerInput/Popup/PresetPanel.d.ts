import * as React from 'react';
import type { ValueDate } from '../../interface';
export interface PresetPanelProps<ValueType = any> {
    prefixCls: string;
    presets: ValueDate<ValueType>[];
    onClick: (value: ValueType) => void;
    onHover: (value: ValueType) => void;
}
export default function PresetPanel<DateType extends object = any>(props: PresetPanelProps<DateType>): React.JSX.Element;
