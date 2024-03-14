import type { FormatType, InternalMode, Locale, SharedPickerProps } from '../../interface';
export declare function useFieldFormat<DateType = any>(picker: InternalMode, locale: Locale, format?: SharedPickerProps['format']): [formatList: FormatType<DateType>[], maskFormat?: string];
