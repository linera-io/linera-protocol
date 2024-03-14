import type { GenerateConfig } from '../generate';
import type { InternalMode, Locale } from '../interface';
/**
 * Toggles the presence of a value in an array.
 * If the value exists in the array, removed it.
 * Else add it.
 */
export default function useToggleDates<DateType>(generateConfig: GenerateConfig<DateType>, locale: Locale, panelMode: InternalMode): (list: DateType[], target: DateType) => DateType[];
