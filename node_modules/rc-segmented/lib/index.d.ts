import * as React from 'react';
export type SegmentedValue = string | number;
export type SegmentedRawOption = SegmentedValue;
export interface SegmentedLabeledOption<ValueType = SegmentedRawOption> {
    className?: string;
    disabled?: boolean;
    label: React.ReactNode;
    value: ValueType;
    /**
     * html `title` property for label
     */
    title?: string;
}
type SegmentedOptions<T = SegmentedRawOption> = (T | SegmentedLabeledOption<T>)[];
export interface SegmentedProps<ValueType = SegmentedValue> extends Omit<React.HTMLProps<HTMLDivElement>, 'defaultValue' | 'value' | 'onChange'> {
    options: SegmentedOptions<ValueType>;
    defaultValue?: ValueType;
    value?: ValueType;
    onChange?: (value: ValueType) => void;
    disabled?: boolean;
    prefixCls?: string;
    direction?: 'ltr' | 'rtl';
    motionName?: string;
}
declare const Segmented: React.ForwardRefExoticComponent<Omit<SegmentedProps<SegmentedValue>, "ref"> & React.RefAttributes<HTMLDivElement>>;
declare const TypedSegmented: <ValueType>(props: SegmentedProps<ValueType> & {
    ref?: React.ForwardedRef<HTMLDivElement> | undefined;
}) => ReturnType<typeof Segmented>;
export default TypedSegmented;
