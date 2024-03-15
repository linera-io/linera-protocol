import type { GenerateConfig } from '../../generate';
import type { PanelMode, RangeTimeProps, SharedPickerProps, SharedTimeProps } from '../../interface';
/**
 * Check if provided date is valid for the `disabledDate` & `showTime.disabledTime`.
 */
export default function useInvalidate<DateType extends object = any>(generateConfig: GenerateConfig<DateType>, picker: PanelMode, disabledDate?: SharedPickerProps<DateType>['disabledDate'], showTime?: SharedTimeProps<DateType> | RangeTimeProps<DateType>): (date: DateType, info?: {
    from?: DateType;
    activeIndex: number;
}) => boolean;
