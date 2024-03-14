/**
 * Sync value with state.
 * This should only used for internal which not affect outside calculation.
 * Since it's not safe for suspense.
 */
export default function useSyncState<T>(defaultValue: T, controlledValue?: T): [getter: (useControlledValueFirst?: boolean) => T, setter: (nextValue: T) => void, value: T];
