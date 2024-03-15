import type { GenerateConfig } from '../../generate';
import type { DisabledDate, Locale } from '../../interface';
import type { RangeValueType } from '../RangePicker';
/**
 * RangePicker need additional logic to handle the `disabled` case. e.g.
 * [disabled, enabled] should end date not before start date
 */
export default function useRangeDisabledDate<DateType extends object = any>(values: RangeValueType<DateType>, disabled: [boolean, boolean], activeIndexList: number[], generateConfig: GenerateConfig<DateType>, locale: Locale, disabledDate?: DisabledDate<DateType>): DisabledDate<DateType>;
