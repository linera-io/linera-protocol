type Updater<T> = T | ((origin: T) => T);
type UpdateCallbackFunc = VoidFunction;
type NotifyEffectUpdate = (callback: UpdateCallbackFunc) => void;
/**
 * Batcher for record any `useEffectState` need update.
 */
export declare function useBatcher(): NotifyEffectUpdate;
/**
 * Trigger state update by `useLayoutEffect` to save perf.
 */
export default function useEffectState<T extends string | number | object>(notifyEffectUpdate: NotifyEffectUpdate, defaultValue?: T): [T, (value: Updater<T>) => void];
export {};
