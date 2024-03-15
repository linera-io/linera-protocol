type Updater<T> = (prev: T) => T;
export default function useSyncState<T>(defaultState: T, onChange: (newValue: T, prevValue: T) => void): [T, (updater: T | Updater<T>) => void];
export {};
