import type { GenerateConfig } from '../../generate';
import type { InternalMode, Locale, PanelMode } from '../../interface';
import type { RangePickerProps } from '../RangePicker';
export declare function offsetPanelDate<DateType = any>(generateConfig: GenerateConfig<DateType>, picker: InternalMode, date: DateType, offset: number): DateType;
export default function useRangePickerValue<DateType extends object, ValueType extends DateType[]>(generateConfig: GenerateConfig<DateType>, locale: Locale, calendarValue: ValueType, modes: PanelMode[], open: boolean, activeIndex: number, pickerMode: InternalMode, multiplePanel: boolean, defaultPickerValue?: ValueType, pickerValue?: ValueType, timeDefaultValue?: ValueType, onPickerValueChange?: RangePickerProps<DateType>['onPickerValueChange'], minDate?: DateType, maxDate?: DateType): [currentIndexPickerValue: DateType, setCurrentIndexPickerValue: (value: DateType) => void];
