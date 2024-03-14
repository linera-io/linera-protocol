type Updater<ValueType> = (prev?: ValueType) => ValueType;
export default function useFrameState<ValueType>(defaultValue: ValueType): [ValueType, (updater: Updater<ValueType>) => void];
export {};
