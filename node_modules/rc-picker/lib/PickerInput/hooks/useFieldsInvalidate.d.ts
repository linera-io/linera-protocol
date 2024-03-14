import type useInvalidate from './useInvalidate';
/**
 * Used to control each fields invalidate status
 */
export default function useFieldsInvalidate<DateType extends object, ValueType extends DateType[]>(calendarValue: ValueType, isInvalidateDate: ReturnType<typeof useInvalidate<DateType>>, allowEmpty?: boolean[]): readonly [[boolean, boolean], (invalid: boolean, index: number) => void];
