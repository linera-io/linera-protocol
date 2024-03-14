import * as React from 'react';
import type { BaseInfo, PanelMode, PickerRef, SharedPickerProps, SharedTimeProps, ValueDate } from '../interface';
export interface BasePickerProps<DateType extends object = any> extends SharedPickerProps<DateType> {
    id?: string;
    /** Not support `time` or `datetime` picker */
    multiple?: boolean;
    removeIcon?: React.ReactNode;
    /** Only work when `multiple` is in used */
    maxTagCount?: number | 'responsive';
    value?: DateType | DateType[] | null;
    defaultValue?: DateType | DateType[];
    onChange?: (date: DateType | DateType[], dateString: string | string[]) => void;
    onCalendarChange?: (date: DateType | DateType[], dateString: string | string[], info: BaseInfo) => void;
    /**  */
    onOk?: (value?: DateType | DateType[]) => void;
    placeholder?: string;
    /**
     * Config the popup panel date.
     * Every time active the input to open popup will reset with `defaultPickerValue`.
     *
     * Note: `defaultPickerValue` priority is higher than `value` for the first open.
     */
    defaultPickerValue?: DateType | null;
    /**
     * Config each start & end field popup panel date.
     * When config `pickerValue`, you must also provide `onPickerValueChange` to handle changes.
     */
    pickerValue?: DateType | null;
    /**
     * Each popup panel `pickerValue` change will trigger the callback.
     * @param date The changed picker value
     * @param info.source `panel` from the panel click. `reset` from popup open or field typing.
     */
    onPickerValueChange?: (date: DateType, info: {
        source: 'reset' | 'panel';
        mode: PanelMode;
    }) => void;
    presets?: ValueDate<DateType>[];
    disabled?: boolean;
    mode?: PanelMode;
    onPanelChange?: (values: DateType, modes: PanelMode) => void;
}
export interface PickerProps<DateType extends object = any> extends BasePickerProps<DateType>, Omit<SharedTimeProps<DateType>, 'format' | 'defaultValue'> {
}
/** Internal usage. For cross function get same aligned props */
export type ReplacedPickerProps<DateType extends object = any> = {
    onChange?: (date: DateType | DateType[], dateString: string | string[]) => void;
    onCalendarChange?: (date: DateType | DateType[], dateString: string | string[], info: BaseInfo) => void;
};
declare const RefPicker: <DateType extends object = any>(props: PickerProps<DateType> & React.RefAttributes<PickerRef>) => React.ReactElement;
export default RefPicker;
