import * as React from 'react';
import type { RangePickerRef, SelectorProps } from '../../interface';
export type SelectorIdType = string | {
    start?: string;
    end?: string;
};
export interface RangeSelectorProps<DateType = any> extends SelectorProps<DateType> {
    id?: SelectorIdType;
    activeIndex: number | null;
    separator?: React.ReactNode;
    value?: [DateType?, DateType?];
    onChange: (date: DateType, index?: number) => void;
    disabled: [boolean, boolean];
    /** All the field show as `placeholder` */
    allHelp: boolean;
    placeholder?: string | [string, string];
    invalid: [boolean, boolean];
    /**
     * Trigger when the active bar offset position changed.
     * This is used for popup panel offset.
     */
    onActiveOffset: (offset: number) => void;
}
declare const RefRangeSelector: React.ForwardRefExoticComponent<RangeSelectorProps<object> & React.RefAttributes<RangePickerRef>>;
export default RefRangeSelector;
