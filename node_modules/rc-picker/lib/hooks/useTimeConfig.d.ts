import type { InternalMode, Locale, SharedPickerProps, SharedTimeProps } from '../interface';
export interface ComponentProps<DateType extends object> {
    picker?: InternalMode;
    showTime?: boolean | Partial<SharedTimeProps<DateType>>;
    locale: Locale;
    format?: SharedPickerProps['format'];
}
/**
 * Get `showHour`, `showMinute`, `showSecond` or other from the props.
 * This is pure function, will not get `showXXX` from the `format` prop.
 */
export declare function getTimeProps<DateType extends object>(componentProps: ComponentProps<DateType>): [
    showTimeProps: SharedTimeProps<DateType>,
    showTimePropsForLocale: SharedTimeProps<DateType>,
    showTimeFormat: string,
    propFormat: string
];
export declare function fillShowTimeConfig<DateType extends object>(picker: InternalMode, showTimeFormat: string, propFormat: string, timeConfig: SharedTimeProps<DateType>, locale: Locale): SharedTimeProps<DateType>;
