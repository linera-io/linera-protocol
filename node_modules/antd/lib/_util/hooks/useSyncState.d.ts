type UseSyncStateProps<T> = readonly [() => T, (newValue: T) => void];
export default function useSyncState<T>(initialValue: T): UseSyncStateProps<T>;
export {};
