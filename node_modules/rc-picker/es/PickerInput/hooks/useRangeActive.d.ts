import type { RangeValueType } from '../RangePicker';
export type OperationType = 'input' | 'panel';
export type NextActive<DateType> = (nextValue: RangeValueType<DateType>) => number | null;
/**
 * When user first focus one input, any submit will trigger focus another one.
 * When second time focus one input, submit will not trigger focus again.
 * When click outside to close the panel, trigger event if it can trigger onChange.
 */
export default function useRangeActive<DateType>(disabled: boolean[], empty?: boolean[]): [
    focused: boolean,
    triggerFocus: (focused: boolean) => void,
    lastOperation: (type?: OperationType) => OperationType,
    activeIndex: number,
    setActiveIndex: (index: number) => void,
    nextActiveIndex: NextActive<DateType>,
    activeList: number[]
];
