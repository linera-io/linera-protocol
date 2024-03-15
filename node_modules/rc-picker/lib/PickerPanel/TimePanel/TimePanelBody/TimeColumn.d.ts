import * as React from 'react';
export type Unit<ValueType = number | string> = {
    label: React.ReactText;
    value: ValueType;
    disabled?: boolean;
};
export interface TimeUnitColumnProps {
    units: Unit[];
    value: number | string;
    optionalValue?: number | string;
    type: 'hour' | 'minute' | 'second' | 'millisecond' | 'meridiem';
    onChange: (value: number | string) => void;
    onDblClick?: VoidFunction;
    changeOnScroll?: boolean;
}
export default function TimeColumn<DateType extends object>(props: TimeUnitColumnProps): React.JSX.Element;
