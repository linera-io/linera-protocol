import * as React from 'react';
import type { CellRender, Components, Locale, OnPanelChange, PanelMode, PickerMode, SharedPanelProps, SharedTimeProps } from '../interface';
export interface PickerPanelRef {
    nativeElement: HTMLDivElement;
}
export interface BasePickerPanelProps<DateType extends object = any> extends Pick<SharedPanelProps<DateType>, 'locale' | 'generateConfig' | 'disabledDate' | 'minDate' | 'maxDate' | 'prevIcon' | 'nextIcon' | 'superPrevIcon' | 'superNextIcon'>, SharedTimeProps<DateType>, Pick<React.HTMLAttributes<HTMLDivElement>, 'tabIndex'> {
    prefixCls?: string;
    direction?: 'ltr' | 'rtl';
    onSelect?: (date: DateType) => void;
    defaultPickerValue?: DateType | null;
    pickerValue?: DateType | null;
    onPickerValueChange?: (date: DateType) => void;
    mode?: PanelMode;
    /**
     * Compatible with origin API.
     * Not mean the PickerPanel `onChange` event.
     */
    onPanelChange?: OnPanelChange<DateType>;
    picker?: PickerMode;
    showTime?: true | SharedTimeProps<DateType>;
    /**
     * Only worked in `date` mode. Show the current week
     */
    showWeek?: boolean;
    cellRender?: CellRender<DateType>;
    /** @deprecated use cellRender instead of dateRender */
    dateRender?: (currentDate: DateType, today: DateType) => React.ReactNode;
    /** @deprecated use cellRender instead of monthCellRender */
    monthCellRender?: (currentDate: DateType, locale: Locale) => React.ReactNode;
    /** @private Used for Picker passing */
    hoverValue?: DateType[];
    /** @private Used for Picker passing */
    hoverRangeValue?: [start: DateType, end: DateType];
    /** @private Used for Picker passing */
    onHover?: (date: DateType) => void;
    components?: Components;
    /** @private This is internal usage. Do not use in your production env */
    hideHeader?: boolean;
}
export interface SinglePickerPanelProps<DateType extends object = any> extends BasePickerPanelProps<DateType> {
    multiple?: false;
    defaultValue?: DateType | null;
    value?: DateType | null;
    onChange?: (date: DateType) => void;
}
export type PickerPanelProps<DateType extends object = any> = BasePickerPanelProps<DateType> & {
    /** multiple selection. Not support time or datetime picker */
    multiple?: boolean;
    defaultValue?: DateType | DateType[] | null;
    value?: DateType | DateType[] | null;
    onChange?: (date: DateType | DateType[]) => void;
};
declare const _default: <DateType extends object = any>(props: BasePickerPanelProps<DateType> & {
    /** multiple selection. Not support time or datetime picker */
    multiple?: boolean;
    defaultValue?: DateType | DateType[];
    value?: DateType | DateType[];
    onChange?: (date: DateType | DateType[]) => void;
} & React.RefAttributes<PickerPanelRef>) => React.ReactElement;
export default _default;
