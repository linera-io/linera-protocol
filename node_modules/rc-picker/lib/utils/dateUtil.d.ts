import type { GenerateConfig } from '../generate';
import type { CustomFormat, InternalMode, Locale, NullableDateType } from '../interface';
export declare const WEEK_DAY_COUNT = 7;
export declare function isSameDecade<DateType>(generateConfig: GenerateConfig<DateType>, decade1: NullableDateType<DateType>, decade2: NullableDateType<DateType>): boolean;
export declare function isSameYear<DateType>(generateConfig: GenerateConfig<DateType>, year1: NullableDateType<DateType>, year2: NullableDateType<DateType>): boolean;
export declare function getQuarter<DateType>(generateConfig: GenerateConfig<DateType>, date: DateType): number;
export declare function isSameQuarter<DateType>(generateConfig: GenerateConfig<DateType>, quarter1: NullableDateType<DateType>, quarter2: NullableDateType<DateType>): boolean;
export declare function isSameMonth<DateType>(generateConfig: GenerateConfig<DateType>, month1: NullableDateType<DateType>, month2: NullableDateType<DateType>): boolean;
export declare function isSameDate<DateType>(generateConfig: GenerateConfig<DateType>, date1: NullableDateType<DateType>, date2: NullableDateType<DateType>): boolean;
export declare function isSameTime<DateType>(generateConfig: GenerateConfig<DateType>, time1: NullableDateType<DateType>, time2: NullableDateType<DateType>): boolean;
/**
 * Check if the Date is all the same of timestamp
 */
export declare function isSameTimestamp<DateType>(generateConfig: GenerateConfig<DateType>, time1: NullableDateType<DateType>, time2: NullableDateType<DateType>): boolean;
export declare function isSameWeek<DateType>(generateConfig: GenerateConfig<DateType>, locale: string, date1: NullableDateType<DateType>, date2: NullableDateType<DateType>): boolean;
export declare function isSame<DateType = any>(generateConfig: GenerateConfig<DateType>, locale: Locale, source: NullableDateType<DateType>, target: NullableDateType<DateType>, type: InternalMode): boolean;
/** Between in date but not equal of date */
export declare function isInRange<DateType>(generateConfig: GenerateConfig<DateType>, startDate: NullableDateType<DateType>, endDate: NullableDateType<DateType>, current: NullableDateType<DateType>): boolean;
export declare function isSameOrAfter<DateType>(generateConfig: GenerateConfig<DateType>, locale: Locale, date1: NullableDateType<DateType>, date2: NullableDateType<DateType>, type: InternalMode): boolean;
export declare function getWeekStartDate<DateType>(locale: string, generateConfig: GenerateConfig<DateType>, value: DateType): DateType;
export declare function formatValue<DateType>(value: DateType, { generateConfig, locale, format, }: {
    generateConfig: GenerateConfig<DateType>;
    locale: Locale;
    format: string | CustomFormat<DateType>;
}): string;
/**
 * Fill the time info into Date if provided.
 */
export declare function fillTime<DateType>(generateConfig: GenerateConfig<DateType>, date: DateType, time?: DateType): DateType;
