import type { GenerateConfig } from '../../generate';
import type { DisabledDate, InternalMode, Locale } from '../../interface';
export type IsInvalidBoundary<DateType> = (currentDate: DateType, type: InternalMode, fromDate?: DateType) => boolean;
/**
 * Merge `disabledDate` with `minDate` & `maxDate`.
 */
export default function useDisabledBoundary<DateType extends object = any>(generateConfig: GenerateConfig<DateType>, locale: Locale, disabledDate?: DisabledDate<DateType>, minDate?: DateType, maxDate?: DateType): DisabledDate<DateType>;
