import type { ValueDate } from '../../interface';
export default function usePresets<DateType = any>(presets?: ValueDate<DateType>[], legacyRanges?: Record<string, DateType | (() => DateType)>): ValueDate<DateType>[];
