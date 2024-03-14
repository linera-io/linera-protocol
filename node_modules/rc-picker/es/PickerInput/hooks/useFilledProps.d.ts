import type { FormatType, InternalMode, PickerMode } from '../../interface';
import type { RangePickerProps } from '../RangePicker';
import useInvalidate from './useInvalidate';
type UseInvalidate<DateType extends object = any> = typeof useInvalidate<DateType>;
type PickedProps<DateType extends object = any> = Pick<RangePickerProps<DateType>, 'generateConfig' | 'locale' | 'picker' | 'prefixCls' | 'styles' | 'classNames' | 'order' | 'components' | 'inputRender' | 'clearIcon' | 'allowClear' | 'needConfirm' | 'format' | 'inputReadOnly' | 'disabledDate' | 'minDate' | 'maxDate' | 'defaultOpenValue'> & {
    multiple?: boolean;
    showTime?: any;
    value?: any;
    defaultValue?: any;
    pickerValue?: any;
    defaultPickerValue?: any;
};
type ExcludeBooleanType<T> = T extends boolean ? never : T;
type GetGeneric<T> = T extends PickedProps<infer U> ? U : never;
type ToArrayType<T, DateType> = T extends any[] ? T : DateType[];
/**
 * Align the outer props with unique typed and fill undefined props.
 * This is shared with both RangePicker and Picker. This will do:
 * - Convert `value` & `defaultValue` to array
 * - handle the legacy props fill like `clearIcon` + `allowClear` = `clearIcon`
 */
export default function useFilledProps<InProps extends PickedProps, DateType extends GetGeneric<InProps>, UpdaterProps extends object>(props: InProps, updater?: () => UpdaterProps): [
    filledProps: Omit<InProps, keyof UpdaterProps | 'showTime' | 'value' | 'defaultValue'> & UpdaterProps & {
        picker: PickerMode;
        showTime?: ExcludeBooleanType<InProps['showTime']>;
        value?: ToArrayType<InProps['value'], DateType>;
        defaultValue?: ToArrayType<InProps['value'], DateType>;
        pickerValue?: ToArrayType<InProps['value'], DateType>;
        defaultPickerValue?: ToArrayType<InProps['value'], DateType>;
    },
    internalPicker: InternalMode,
    complexPicker: boolean,
    formatList: FormatType<DateType>[],
    maskFormat: string,
    isInvalidateDate: ReturnType<UseInvalidate<DateType>>
];
export {};
