import type { Locale, SharedTimeProps } from '../interface';
export declare function fillTimeFormat(showHour: boolean, showMinute: boolean, showSecond: boolean, showMillisecond: boolean, showMeridiem: boolean): string;
/**
 * Fill locale format as start up
 */
export default function useLocale<DateType extends object>(locale: Locale, showProps: Pick<SharedTimeProps<DateType>, 'showHour' | 'showMinute' | 'showSecond' | 'showMillisecond' | 'use12Hours'>): Locale;
